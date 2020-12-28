package org.opendc.experiments.allocateam.policies.elop

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.job.JobAdmissionPolicy

/**
 * A [JobAdmissionPolicy] that reserves machines based on the Level of Parallelism of the Workflow.
 *
 * @property reservedNodes The machines that are reserved for per active job.
 */
public class ELOPJobAdmissionPolicy(private val reservedNodes: MutableMap<JobState, MutableList<Node>>) : JobAdmissionPolicy {
    override fun invoke(scheduler: StageWorkflowService): JobAdmissionPolicy.Logic = object : JobAdmissionPolicy.Logic {
        override fun invoke(
            job: JobState
        ): JobAdmissionPolicy.Advice {
            freeReservationsForFinishedJobs()

            // Allocate available resources to active jobs that do not have n=LOP reservations.
            for (activeJob in scheduler.activeJobs) {
                reserveIfNeeded(activeJob, scheduler)
            }
            if (reserveIfNeeded(job, scheduler)) {
                // Admit job if some resources have been able to be reserved for job at head of queue.
                return JobAdmissionPolicy.Advice.ADMIT
            }
            return JobAdmissionPolicy.Advice.DENY
        }
    }

    /**
     * True if at least one node has successfully been reserved for the job.
     */
    private fun reserveIfNeeded(job: JobState, scheduler: StageWorkflowService): Boolean {
        val levelOfParallelism = calculateLOP(job)
        val currentAmountOfReservationsForJob = getCurrentAmountOfReservationsForJob(job)
        val canAndShouldReserveForJob = (
            shouldReserveNodes(currentAmountOfReservationsForJob, levelOfParallelism, scheduler.available.toList())
            )
        if (canAndShouldReserveForJob) {
            val amountToReserve = getAmountToReserve(currentAmountOfReservationsForJob, levelOfParallelism)
            reserveNodes(amountToReserve, scheduler.available.toList(), job)
            return true
        }
        return false
    }

    private fun freeReservationsForFinishedJobs() {
        val jobsToRemove = mutableListOf<JobState>()
        for ((job, _) in this.reservedNodes) {
            if (job.isFinished) {
                jobsToRemove.add(job)
            }
        }
        this.reservedNodes.keys.removeAll(jobsToRemove)
    }

    private fun calculateLOP(job: JobState): Int {
        return LevelOfParallelismCalculator().calculateLOP(job)
    }

    private fun getCurrentAmountOfReservationsForJob(job: JobState): Int {
        val reservedNodesForJob: List<Node> = this.reservedNodes[job] ?: mutableListOf()
        return reservedNodesForJob.size
    }

    private fun shouldReserveNodes(currentAmountOfReservations: Int, levelOfParallelism: Int, availableNodes: List<Node>): Boolean {
        val availableAndNotReservedNodes = getNonReservedNodes(availableNodes)
        return (levelOfParallelism > currentAmountOfReservations &&
            availableAndNotReservedNodes.size > currentAmountOfReservations)
    }

    private fun getAmountToReserve(currentAmountOfReservations: Int, levelOfParallelism: Int): Int {
        return levelOfParallelism - currentAmountOfReservations
    }

    private fun getNonReservedNodes(availableNodes: List<Node>): List<Node> {
        val unavailableNodes: MutableSet<Node> = mutableSetOf()

        for ((_, nodes) in this.reservedNodes) {
            unavailableNodes.addAll(nodes)
        }
        return availableNodes.minus(unavailableNodes)
    }

    private fun reserveNodes(amountToReserve: Int, availableNodes: List<Node>, job: JobState) {
        fun reserveNodeForJob(job: JobState, node: Node) {
            val currentlyReservedNodesForJob = this.reservedNodes[job] ?: mutableListOf()
            currentlyReservedNodesForJob.add(node)
            this.reservedNodes[job] = currentlyReservedNodesForJob
        }
        for (i in 1..amountToReserve) {
            val availableNonReservedNodes: List<Node> = getNonReservedNodes(availableNodes)
            if (availableNonReservedNodes.isNotEmpty()) {
                val toReserve = availableNonReservedNodes.first()
                reserveNodeForJob(job, toReserve)
            }
        }
    }

    override fun toString(): String = "ELoP"
}

public class LevelOfParallelismCalculator {
    private var rootTasks = mutableSetOf<TaskState>()
    private var step = 1
    private var countedTokensPerStep = mutableMapOf<Int, Int>()
    private var currentNodesWithTokens = mutableSetOf<TaskState>()
    private var previousNodesWithTokens = mutableSetOf<TaskState>()

    private fun resetProperties() {
        rootTasks = mutableSetOf()
        step = 1
        countedTokensPerStep = mutableMapOf()
        currentNodesWithTokens = mutableSetOf()
        previousNodesWithTokens = mutableSetOf()
    }

    /**
     * Calculate the (maximum) Level of Parallelism for the DAG/Job/Workflow.
     */
    public fun calculateLOP(job: JobState): Int {
        fun getRootTasks(job: JobState) {
            for (task in job.tasks) {
                if (task.dependencies.isEmpty()) {
                    rootTasks.add(task)
                }
            }
        }

        /**
         * A 'step' in the algorithm that adds tokens to all successive nodes of the nodes that
         * currently 'have' a token.
         */
        fun addTokensToChildren() {
            previousNodesWithTokens = currentNodesWithTokens
            currentNodesWithTokens = mutableSetOf()
            for (previousNodeWithToken in previousNodesWithTokens) {
                currentNodesWithTokens.addAll(previousNodeWithToken.dependents)
            }
            step++
        }

        fun countTokens() {
            countedTokensPerStep[step] = currentNodesWithTokens.size
        }

        fun moreChildrenExist(): Boolean {
            for (node in currentNodesWithTokens) {
                if (node.dependents.size != 0) {
                    return true
                }
            }
            return false
        }

        fun getMaxCountedTokens(): Int {
            var maxLOP = 0
            for ((_, countedLOP) in countedTokensPerStep) {
                maxLOP = if (countedLOP > maxLOP) countedLOP else maxLOP
            }
            return maxLOP
        }
        resetProperties()

        // 1. add tokens to entry nodes of DAG
        getRootTasks(job)
        for (rootTask in rootTasks) {
            currentNodesWithTokens.add(rootTask)
        }
        countTokens()

        // 2. move tokens to successive nodes
        while (moreChildrenExist()) {
            addTokensToChildren()
            countTokens()
        }
        return getMaxCountedTokens()
    }
}
