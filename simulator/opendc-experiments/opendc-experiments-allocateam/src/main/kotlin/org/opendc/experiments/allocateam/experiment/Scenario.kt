package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.experiment.model.Topology
import org.opendc.experiments.sc20.experiment.model.Workload
import org.opendc.experiments.sc20.runner.ContainerExperimentDescriptor
import org.opendc.experiments.sc20.runner.ExperimentDescriptor

/**
 * A scenario represents a single point in the design space (a unique combination of parameters).
 */
public class Scenario(
    override val parent: Portfolio,
    public val id: Int,
    public val repetitions: Int,
    public val topology: Topology,
    public val workload: Workload,
    public val resourceAllocationPolicy: String,
) : ContainerExperimentDescriptor() {
    override val children: Sequence<ExperimentDescriptor> = sequence {
        repeat(repetitions) {
            i -> yield(Run(this@Scenario, i, i))
        }
    }
}
