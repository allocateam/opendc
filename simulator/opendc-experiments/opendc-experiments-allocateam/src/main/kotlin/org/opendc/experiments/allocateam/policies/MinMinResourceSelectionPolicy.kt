package org.opendc.experiments.allocateam.policies

import org.opendc.compute.core.metal.Node
import org.opendc.compute.simulator.SimWorkloadImage
import org.opendc.simulator.compute.workload.SimFlopsWorkload
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy

public class MinMinResourceSelectionPolicy(public val flopsPerCore: Long) : ResourceSelectionPolicy {
    override fun invoke(scheduler: StageWorkflowService): (List<Node>, TaskState) -> Node? {
        return { availableNodes, taskState ->
            var minimumCompletionTime = Long.MAX_VALUE
            var chosenNode: Node? = availableNodes.firstOrNull()

            for (node in availableNodes) {
                if (taskState.task.image is SimWorkloadImage) {
                    if ((taskState.task.image as SimWorkloadImage).workload is SimFlopsWorkload) {
                        val time = ((taskState.task.image as SimWorkloadImage).workload as SimFlopsWorkload).flops / flopsPerCore

                        if (time < minimumCompletionTime) {
                            chosenNode = node
                            minimumCompletionTime = time
                        }
                    }
                }
            }

            chosenNode
        }
    }

    override fun toString(): String = "MinMin"

}
