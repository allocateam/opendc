package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.experiment.Experiment
import org.opendc.experiments.sc20.experiment.model.Topology
import org.opendc.experiments.sc20.experiment.model.Workload

public class SmokeTestPortfolio(parent: Experiment, id: Int) : Portfolio(parent, id, "smokeTest") {
    override val topologies: List<Topology> = listOf(
        Topology("single")
    )

    override val workloads: List<Workload> = listOf(
        // Workload("pegasus_p4", 1.0),
        Workload("bitbrains-small", 1.0)
    )

    override val allocationPolicies: List<String> = listOf(
        "mem"
    )
}
