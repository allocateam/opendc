package org.opendc.experiments.allocateam.policies

import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy

public object MinMaxResourceSelectionPolicy : ResourceSelectionPolicy {

    // TODO(gm): this is only a placeholder for the moment
    override fun invoke(scheduler: StageWorkflowService): Comparator<Node> =
        Comparator { _, _ -> 1 }

    override fun toString(): String = "MinMax"

}
