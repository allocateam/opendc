package org.opendc.experiments.allocateam.policies

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy

public object MinMaxResourceSelectionPolicy : ResourceSelectionPolicy {

    // TODO(gm): this is only a placeholder for the moment
    override fun invoke(scheduler: StageWorkflowService): Comparator<Node> {
        return object : Comparator<Node> {
            override fun compare(o1: Node, o2: Node): Int {
//                println(o1)
//                println(o2)

                return 1
            }
        }
    }

    override fun toString(): String = "MinMax"
}
