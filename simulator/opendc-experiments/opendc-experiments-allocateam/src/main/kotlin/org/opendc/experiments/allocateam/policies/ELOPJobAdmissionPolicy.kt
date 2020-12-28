package org.opendc.experiments.allocateam.policies

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.job.JobAdmissionPolicy

public class ELOPJobAdmissionPolicy(private val reservedNodes: MutableMap<JobState, MutableList<Node>>) : JobAdmissionPolicy {

    // The nodes that are reserved per Workflow (JobState)
//    private var reservedNodes: MutableMap<JobState, MutableList<Node>> = mutableMapOf()

    private fun printReservedNodes() {
        println("\n###\nPrinting reserved nodes:")
        for ((job, nodes) in reservedNodes) {
            println("\tJob: ${job.job.uid}")
            println("\tn_nodes: ${nodes.size}")
            for (node in nodes) {
                println("\t\t node.uid: ${node.uid}")
            }
        }
        println("Completed printing reserved nodes\n###\n")
    }

    override fun invoke(scheduler: StageWorkflowService): JobAdmissionPolicy.Logic = object : JobAdmissionPolicy.Logic {
        override fun invoke(
            job: JobState
        ): JobAdmissionPolicy.Advice {
            println("\n-----\nInvoking ELOP!\n-----\n")
            printReservedNodes()
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

    private fun reserveIfNeeded(job: JobState, scheduler: StageWorkflowService): Boolean {
        val levelOfParallelism = calculateLOP(job)
        val currentAmountOfReservationsForJob = getCurrentAmountOfReservationsForJob(job)
        if (shouldReserveNodes(currentAmountOfReservationsForJob, levelOfParallelism, scheduler.available.toList())) {
            val amountToReserve = getAmountToReserve(currentAmountOfReservationsForJob, levelOfParallelism)
            println("Reserving $amountToReserve nodes for job ${job.job.uid}")
            reserveNodes(amountToReserve, scheduler.available.toList(), job)
            printReservedNodes()
            return true
        }
        return false
    }

    private fun freeReservationsForFinishedJobs() {
        for ((job, _) in this.reservedNodes) {
            if (job.isFinished) {
                val removed = this.reservedNodes.remove(job)
                print("Job ${job.job.uid} finished. Removing ${removed?.size} reserved node(s) for job ${job.job.uid}")
            }
        }
    }

    private fun getNonReservedNodes(availableNodes: List<Node>): List<Node> {
        var unavailableNodes: MutableSet<Node> = mutableSetOf()

        for ((_, nodes) in this.reservedNodes) {
            unavailableNodes.addAll(nodes)
        }
        return availableNodes.minus(unavailableNodes)
    }

    private fun calculateLOP(job: JobState): Int {
        val lop = LevelOfParallelismCalculator().calculateLOP(job)
        return lop
    }

    private fun getCurrentAmountOfReservationsForJob(job: JobState): Int {
        val reservedNodesForJob: List<Node> = this.reservedNodes[job] ?: mutableListOf()
        return reservedNodesForJob.size
    }

    private fun shouldReserveNodes(currentAmountOfReservations: Int, levelOfParallelism: Int, availableNodes: List<Node>): Boolean {
        return levelOfParallelism > currentAmountOfReservations && availableNodes.size > currentAmountOfReservations
    }

    private fun getAmountToReserve(currentAmountOfReservations: Int, levelOfParallelism: Int): Int {
        return levelOfParallelism - currentAmountOfReservations
    }

    private fun reserveNodes(amountToReserve: Int, availableNodes: List<Node>, job: JobState) {
        fun reserveNodeForJob(job: JobState, node: Node) {
            val currentlyReservedNodesForJob = this.reservedNodes[job] ?: mutableListOf()
            currentlyReservedNodesForJob.add(node)
            this.reservedNodes[job] = currentlyReservedNodesForJob
        }
        for (i in 1..amountToReserve) {
            val nodeToReserve: Node = this.getNonReservedNodes(availableNodes).first()
            reserveNodeForJob(job, nodeToReserve)
            availableNodes - nodeToReserve
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

    public fun calculateLOP(job: JobState): Int {
        /**
         * Calculate the (maximum) Level of Parallelism for the DAG/Job/Workflow.
         */
        fun getRootTasks(job: JobState) {
            for (task in job.tasks) {
                if (task.dependencies.isEmpty()) {
                    rootTasks.add(task)
                }
            }
        }

        fun addTokensToChildren() {
            /**
             * A 'step' in the algorithm that adds tokens to all successive nodes of the nodes that
             * currently 'have' a token.
             */
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
