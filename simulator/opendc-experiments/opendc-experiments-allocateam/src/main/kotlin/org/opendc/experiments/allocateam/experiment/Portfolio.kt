package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.experiment.model.Topology
import org.opendc.experiments.sc20.experiment.model.Workload
import org.opendc.experiments.sc20.runner.ContainerExperimentDescriptor

/**
 * A portfolio represents a collection of scenarios that are tested.
 */
public abstract class Portfolio(
    override val parent: Experiment,
    public val id: Int,
    public val name: String
) : ContainerExperimentDescriptor() {
    /**
     * The topologies to consider.
     */
    protected abstract val topologies: List<Topology>

    /**
     * The workloads to consider.
     */
    protected abstract val workloads: List<Workload>

    /**
     * The allocation policies to consider.
     */
    protected abstract val resourceSelectionPolicy: List<String>

    protected abstract val taskEligibilityPolicy: String

    /**
     * The number of repetitions to perform.
     */
    public open val repetitions: Int = 1

    /**
     * Resolve the children of this container.
     */
    override val children: Sequence<Scenario> = sequence {
        var id = 0
        for (topology in topologies) {
            for (workload in workloads) {
                for (allocationPolicy in resourceSelectionPolicy) {
                    yield(
                        Scenario(
                            this@Portfolio,
                            id++,
                            repetitions,
                            topology,
                            workload,
                            allocationPolicy,
                            taskEligibilityPolicy
                        )
                    )
                }
            }
        }
    }
}
