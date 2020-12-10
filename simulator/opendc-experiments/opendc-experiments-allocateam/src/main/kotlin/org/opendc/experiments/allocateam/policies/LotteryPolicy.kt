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
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.task.TaskEligibilityPolicy
import org.opendc.workflows.workload.Task
import kotlin.random.Random

private val logger = KotlinLogging.logger {}


/**
 * A [TaskEligibilityPolicy] that limits the number of active tasks of a job in the system.
 */
public data class LotteryPolicy(public val lotteryRounds: Int) : TaskEligibilityPolicy {

    override fun invoke(scheduler: StageWorkflowService): TaskEligibilityPolicy.Logic =
        object : TaskEligibilityPolicy.Logic, StageWorkflowSchedulerListener {

            private var numScheduledSoFar = 0
            private var tasksList: MutableList<Task> = mutableListOf()
            private var numJobsStarted = 0
            private var numJobsFinished = 0
            private var numCycles = 0

            init {
                scheduler.addListener(this)
            }

            override fun cycleStarted(scheduler: StageWorkflowService) {
                logger.info("Cycle ${++numCycles} started")
            }

            fun rand(start: Int, end: Int): Int {
                require(!(start > end || end - start + 1 > Int.MAX_VALUE)) { "Illegal Argument" }
                return Random(System.nanoTime()).nextInt(end - start + 1) + start
            }

            override fun jobStarted(job: JobState) {
                logger.info("Job started: ${job.job.uid} number of jobs started: ${++numJobsStarted}")
            }

            override fun jobFinished(job: JobState) {
                logger.info("Job finished: ${job.job.uid} number of jobs finished: ${++numJobsFinished}")
            }

            override fun taskReady(task: TaskState) {
                tasksList.add(task.task)
            }

            override fun taskAssigned(task: TaskState) {
                tasksList.remove(task.task)
            }

            override fun invoke(task: TaskState): TaskEligibilityPolicy.Advice {
                if(numScheduledSoFar + 1 == lotteryRounds) {
                    numScheduledSoFar = 0
                    return TaskEligibilityPolicy.Advice.STOP
                }

                val taskHasDependencies:Boolean = task.task.dependencies.isNotEmpty()
                val lotteryTickets:IntArray =
                    if(taskHasDependencies)
                    IntArray(40){60 + (it + 1)} else //40% probability for tasks with dependencies
                    IntArray(60){it + 1} //60% probability for tasks without dependencies

                val taskLotteryTicket = rand(1, 100)

                if(lotteryTickets.contains(taskLotteryTicket)) {
                    return TaskEligibilityPolicy.Advice.ADMIT
                }

                return TaskEligibilityPolicy.Advice.DENY
            }
        }

    override fun toString(): String = "Round-Robin-Quanta-($lotteryRounds)"
}
