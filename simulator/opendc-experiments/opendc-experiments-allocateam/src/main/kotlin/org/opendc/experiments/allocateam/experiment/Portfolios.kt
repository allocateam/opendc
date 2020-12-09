package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.experiment.model.Topology
import org.opendc.experiments.sc20.experiment.model.Workload

/**
 * Concrete implementations of [Portfolio].
 */

public class SmokeTestPortfolio(parent: Experiment, id: Int) : Portfolio(parent, id, "smokeTest") {
    override val topologies: List<Topology> = listOf(
        Topology("single")
    )

    override val workloads: List<Workload> = listOf(
        Workload("shell", 1.0),
        Workload("askalon_ee", 1.0)
    )

    override val allocationPolicies: List<String> = listOf(
        "min-min",
        "max-min"
    )

    override val repetitions: Int = 4
}
