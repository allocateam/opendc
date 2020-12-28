package org.opendc.experiments.allocateam.policies.heft

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.TaskState


public class HeftPolicyState {

    public data class Event(val task: TaskState?, val start: Long?, val end: Long)

    private val computedTaskRanks = HashMap<TaskState, Long>()
    private val allNodes: MutableSet<Node> = mutableSetOf()

    public val heftOrdersJobs: MutableMap<JobState, MutableMap<Node, MutableList<Event>>> = mutableMapOf()
    public val heftJobsOn: MutableMap<JobState, MutableMap<TaskState, Node>> = mutableMapOf()

    private fun calculateCommunicationCostOfTask(task: TaskState): Long {
        return 0
    }

    private fun calculateRankOfTask(task: TaskState): Long {
        val taskWeight = task.task.runtime

        // Check that avoids infinite loops
        if (task in computedTaskRanks) {
            return computedTaskRanks[task]!!
        }

        val taskRank: Long = if (task.dependents.size > 0)
            taskWeight + task.dependents
                .map { t -> calculateCommunicationCostOfTask(t) + calculateRankOfTask(t) }
                .maxOrNull()!! else
            taskWeight

        computedTaskRanks[task] = taskRank
        return taskRank
    }

    private fun endTime(task: TaskState, events: MutableList<Event>): Long {
        return events.filter { event -> event.task == task }[0].end
    }

    private fun startTime(task: TaskState,
                          orders: MutableMap<Node, MutableList<Event>>,
                          jobsOn: MutableMap<TaskState, Node>,
                          agent: Node): Long {

        try {
            val duration = task.task.runtime
            var commReady: Long = 0

            if (task.dependents.size > 0) {
                commReady = task.dependents
                    .map { t -> if (t in jobsOn) endTime(t, orders[jobsOn[t]]!!) else 0 }
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

        if (agentOrders.isNullOrEmpty()) {
            return desiredStartTime
        }

        val a = mutableListOf(Event(null, null, 0))
        a.addAll(agentOrders.dropLast(1))

        for (e in a zip agentOrders) {
            val e1 = e.first
            val e2 = e.second

            val earliestStart = maxOf(desiredStartTime, e1.end)
            if (e2.start!! - earliestStart > duration) {
                return earliestStart
            }
        }

        return maxOf(agentOrders.last().end, desiredStartTime)
    }

    private fun scheduleTasks(job: JobState, taskRanks: HashMap<TaskState, Long>) {
        val orderedTasksByRanks = taskRanks.toList().sortedBy { p -> p.second }
        val orders: MutableMap<Node, MutableList<Event>> = allNodes.map { it to mutableListOf<Event>() }
            .toMap()
            .toMutableMap()
        val jobsOn: MutableMap<TaskState, Node> = mutableMapOf()
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

    public fun addNodes(nodes: List<Node>) {
        allNodes.addAll(nodes)
    }

    public fun addJob(job: JobState) {
        try {
            val taskRanks = HashMap<TaskState, Long>()
            job.tasks.forEach { task ->
                val rank = calculateRankOfTask(task)
                taskRanks[task] = rank
            }
            scheduleTasks(job, taskRanks)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    public fun removeJob(job: JobState) {
        heftOrdersJobs.remove(job)
        heftJobsOn.remove(job)
    }

    public fun getAgentForTaskState(taskState: TaskState): Node? {
        val job = taskState.job
        val agent = heftJobsOn[job]?.get(taskState)
// val event = heftOrdersJobs[job]?.get(agent)?.filter { event -> event.task == taskState }?.get(0)
//        println("Agent: ${agent?.name}, Job: ${job.job.uid} Event: [${event?.task?.task?.uid}, ${event?.start}, ${event?.end}]")
        return agent
    }
}
