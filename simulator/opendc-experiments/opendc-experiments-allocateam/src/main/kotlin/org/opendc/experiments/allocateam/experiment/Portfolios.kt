package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.experiment.model.Topology
import org.opendc.experiments.sc20.experiment.model.Workload

/**
 * Concrete implementations of [Portfolio].
 */

public class AllocateamPortfolio(parent: Experiment, id: Int) : Portfolio(parent, id, "allocateam") {
    override val topologies: List<Topology> = listOf(
        Topology("single")
    )

    override val workloads: List<Workload> = listOf(
        Workload("shell", 1.0),
        Workload("askalon_ee", 1.0),
        Workload("spec_trace-2", 1.0),
    )

    override val resourceAllocationPolicies: List<String> = listOf(
        "min-min",
        "max-min",
        "round-robin",
        "lottery",
        "heft"
    )

    override val repetitions: Int = 2
}
