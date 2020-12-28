package org.opendc.experiments.allocateam.policies.elop

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy

/**
 * A [ResourceSelectionPolicy] that selects the first machine that is available from the machines that
 * are reserved for the job that the task belongs to.
 */
public class ELOPResourceSelectionPolicy(private val reservedNodes: MutableMap<JobState, MutableList<Node>>) : ResourceSelectionPolicy {
    override fun invoke(scheduler: StageWorkflowService): (List<Node>, TaskState) -> Node? {
        return { availableNodes, taskState ->
            getFirstAvailableReservation(taskState, availableNodes)
        }
    }

    private fun getFirstAvailableReservation(taskState: TaskState, availableNodes: List<Node>): Node? {
        val reservationsForJob: List<Node>? = this.reservedNodes[taskState.job]

        if (reservationsForJob != null) {
            for (reservation in reservationsForJob) {
                if (reservation in availableNodes) {
                    return reservation
                }
            }
        }
        return null
    }

    override fun toString(): String = "ELoP"
}
