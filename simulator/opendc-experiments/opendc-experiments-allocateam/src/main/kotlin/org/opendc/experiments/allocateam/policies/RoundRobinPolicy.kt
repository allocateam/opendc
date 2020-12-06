package org.opendc.experiments.allocateam.policies

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy

public object RoundRobinPolicy : ResourceSelectionPolicy {

    override fun invoke(scheduler: StageWorkflowService): Comparator<Node> {
        return Comparator<Node> { o1, o2 ->
            println(o1)
            println(o2)

            1
        }
    }

    override fun toString(): String = "MinMax"

}
