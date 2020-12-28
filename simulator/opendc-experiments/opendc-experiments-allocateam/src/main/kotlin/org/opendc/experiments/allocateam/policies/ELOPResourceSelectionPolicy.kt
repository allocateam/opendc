package org.opendc.experiments.allocateam.policies

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy

public class ELOPResourceSelectionPolicy(private val reservedNodes: MutableMap<JobState, MutableList<Node>>) : ResourceSelectionPolicy {

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

    override fun invoke(scheduler: StageWorkflowService): (List<Node>, TaskState) -> Node? {
        return { availableNodes, taskState ->
            getFirstAvailableReservation(taskState, availableNodes)
        }
    }

    private fun getFirstAvailableReservation(taskState: TaskState, availableNodes: List<Node>): Node? {
        val reservationsForJob: List<Node>? = this.reservedNodes[taskState.job]
        println("Getting first available reservation for task ${taskState.task.uid} of job ${taskState.job
            .job.uid}\n~~~\n")

        println("First printing reservations for all jobs:")
        printReservedNodes()
        if (reservationsForJob != null) {
            println("(partial) reservations for job:")
            for (reservation in reservationsForJob) {
                if (reservation in availableNodes) {
                    println("Allocating resource: ${reservation.uid}")
                    return reservation
                }
            }
        }
        return null
    }

    override fun toString(): String = "ELoP"
}

//public class LevelOfParallelismCalculator {
//    private var rootTasks = mutableSetOf<TaskState>()
//    private var step = 1
//    private var countedTokensPerStep = mutableMapOf<Int, Int>()
//    private var currentNodesWithTokens = mutableSetOf<TaskState>()
//    private var previousNodesWithTokens = mutableSetOf<TaskState>()
//
//    private fun resetProperties() {
//        rootTasks = mutableSetOf()
//        step = 1
//        countedTokensPerStep = mutableMapOf()
//        currentNodesWithTokens = mutableSetOf()
//        previousNodesWithTokens = mutableSetOf()
//    }
//
//    public fun calculateLOP(task: TaskState): Int {
//        /**
//         * Calculate the (maximum) Level of Parallelism for the DAG/Job/Workflow
//         * that the [TaskState] belongs to.
//         */
//        fun getRootTasks(task: TaskState) {
//            if (task.isRoot) {
//                rootTasks.add(task)
//            } else {
//                for (parent in task.dependents) {
//                    return getRootTasks(parent)
//                }
//            }
//        }
//
//        fun addTokensToChildren() {
//            /**
//             * A 'step' in the algorithm that adds tokens to all successive nodes of the nodes that
//             * currently 'have' a token.
//             */
//            previousNodesWithTokens = currentNodesWithTokens
//            currentNodesWithTokens = mutableSetOf()
//            for (previousNodeWithToken in previousNodesWithTokens) {
//                currentNodesWithTokens.addAll(previousNodeWithToken.dependents)
//            }
//            step++
//        }
//
//        fun countTokens() {
//            countedTokensPerStep[step] = currentNodesWithTokens.size
//        }
//
//        fun moreChildrenExist(): Boolean {
//            for (node in currentNodesWithTokens) {
//                if (node.dependents.size != 0) {
//                    return true
//                }
//            }
//            return false
//        }
//
//        fun getMaxCountedTokens(): Int {
//            var maxLOP = 0
//            for ((_, countedLOP) in countedTokensPerStep) {
//                maxLOP = if (countedLOP > maxLOP) countedLOP else maxLOP
//            }
//            return maxLOP
//        }
//        resetProperties()
//        // 1. add tokens to entry nodes of DAG
//        getRootTasks(task)
//        for (rootTask in rootTasks) {
//            currentNodesWithTokens.add(rootTask)
//        }
//        countTokens()
//        // 2. move tokens to successive nodes
//        while (moreChildrenExist()) {
//            addTokensToChildren()
//            countTokens()
//        }
//        return getMaxCountedTokens()
//    }
//}
