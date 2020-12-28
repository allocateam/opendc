/*
 * Copyright (c) 2020 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.experiments.allocateam.policies

import mu.KotlinLogging
import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy
import org.opendc.workflows.service.stage.task.TaskEligibilityPolicy
import kotlin.collections.HashMap


private val logger = KotlinLogging.logger {}


/**
 * A [TaskEligibilityPolicy] that limits the number of active tasks of a job in the system.
 */
public class HeftPolicy : ResourceSelectionPolicy {

    public data class Event(val task: TaskState?, val start: Long?, val end:Long)



    override fun invoke(scheduler: StageWorkflowService): (List<Node>, TaskState) -> Node? =
        object : (List<Node>, TaskState) -> Node?, StageWorkflowSchedulerListener {

            private val computedTaskRanks = HashMap<TaskState, Long>()
            private val allNodes: MutableSet<Node> = mutableSetOf()

            private val heftOrdersJobs: MutableMap<JobState, MutableMap<Node, MutableList<Event>>> = mutableMapOf()
            private val heftJobsOn: MutableMap<JobState, MutableMap<TaskState, Node>> = mutableMapOf()

            private var jobsStarted = 0
            private var jobsFinished = 0

            init {
                scheduler.addListener(this)
                allNodes.addAll(scheduler.nodes)
            }

            fun calculateCommunicationCostOfTask(task: TaskState): Long {
                return 0
            }

            fun calculateRankOfTask(task: TaskState): Long {
                var taskWeight: Long
                var taskRank: Long

                taskWeight = task.task.runtime

                // Check that avoids infinite loops
                if(task in computedTaskRanks) {
                    return computedTaskRanks[task]!!
                }

                taskRank = if (task.dependents.size > 0)
                    taskWeight + task.dependents
                        .map { t -> calculateCommunicationCostOfTask(t) + calculateRankOfTask(t) }
                        .maxOrNull()!! else
                    taskWeight

                computedTaskRanks[task] = taskRank
                return taskRank
            }

            private fun endTime(task:TaskState, events: MutableList<Event>): Long {
                return events.filter { event -> event.task == task }[0].end
            }

            private fun startTime(task: TaskState,
                                  orders: MutableMap<Node, MutableList<Event>>,
                                  jobsOn: MutableMap<TaskState, Node>,
                                  agent: Node): Long {

                try {
                    val duration = task.task.runtime

                    var commReady: Long = 0

                    if(task.dependents.size > 0) {
                        commReady = task.dependents
                            .map { t -> if(t in jobsOn) endTime(t, orders[jobsOn[t]]!!) else 0 }
                            .maxOrNull()!!
                    }

                    return findFirstGap(orders[agent], commReady, duration)
                } catch (e: Exception) {
                    e.printStackTrace()
                    throw e
                }
            }

            private fun findFirstGap(agentOrders: MutableList<Event>?,
                                     desiredStartTime: Long,
                                     duration: Long): Long {

                if(agentOrders.isNullOrEmpty()) {
                    return desiredStartTime
                }

                val a = mutableListOf<Event>(Event(null, null, 0))
                a.addAll(agentOrders.dropLast(1))

                for(e in a zip agentOrders) {
                    val e1 = e.first
                    val e2 = e.second

                    val earliestStart = maxOf(desiredStartTime, e1.end)
                    if(e2.start!! - earliestStart > duration) {
                        return earliestStart
                    }
                }

                return maxOf(agentOrders.last().end, desiredStartTime)
            }

            override fun jobStarted(job: JobState) {
                try {
                    var taskRanks = HashMap<TaskState, Long>()
                    job.tasks.forEach { task ->
                        val rank = calculateRankOfTask(task)
                        taskRanks[task] = rank
                    }
                    scheduleTasks(job, taskRanks)
                    logger.info("Job ${job.job.uid} started, Num jobs started: ${++jobsStarted}")
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }

            override fun jobFinished(job: JobState) {
                logger.info("Job ${job.job.uid} finished, Num jobs finished: ${++jobsFinished}")
                heftOrdersJobs.remove(job)
                heftJobsOn.remove(job)
            }

            private fun scheduleTasks(job: JobState, taskRanks: HashMap<TaskState, Long>) {
                val orderedTasksByRanks = taskRanks.toList().sortedBy { p -> p.second }
                var orders: MutableMap<Node, MutableList<Event>> = allNodes.map { it to mutableListOf<Event>() }
                    .toMap()
                    .toMutableMap()
                var jobsOn: MutableMap<TaskState, Node> = mutableMapOf()
                allocateTasks(job, orderedTasksByRanks, orders, jobsOn)
            }

            private fun allocateTasks(job: JobState,
                                      orderedTasksByRanks: List<Pair<TaskState, Long>>,
                                      orders: MutableMap<Node, MutableList<Event>>,
                                      jobsOn: MutableMap<TaskState, Node>) {

                orderedTasksByRanks.forEach { p ->
                    val task = p.first
                    val finishTime = { agent: Node -> startTime(task, orders, jobsOn, agent) + task.task.runtime }
                    var agent = orders.keys.toList().minByOrNull(finishTime)
                    val start = startTime(task, orders, jobsOn, agent!!)
                    val end = finishTime(agent)

                    orders.getOrDefault(agent, mutableListOf()).add(Event(task, start, end))
                    orders[agent]!!.sortBy { e -> e.start }

                    jobsOn[task] = agent

                }

                heftOrdersJobs.put(job, orders)
                heftJobsOn.put(job, jobsOn)
            }

            override fun taskAssigned(task: TaskState) {
                return
            }

            override fun invoke(availableNodes: List<Node>, taskState: TaskState): Node? {
                val job = taskState.job

                val agent = heftJobsOn[job]?.get(taskState)

                return if(availableNodes.contains(agent)) {
                    agent
                } else {
                    null
                }
            }

            override fun toString(): String {
                return "HeftPolicy()"
            }
        }
}
