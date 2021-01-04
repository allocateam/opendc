package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.experiment.model.Topology
import org.opendc.experiments.sc20.experiment.model.Workload

/**
 * Concrete implementations of [Portfolio].
 */

public class AllocateamPortfolio(parent: Experiment, id: Int) : Portfolio(parent, id, "allocateam") {
    override val topologies: List<Topology> = listOf(
        Topology("small"),
        Topology("medium"),
        Topology("large")
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
        "heft",
        "elop"
    )

    override val repetitions: Int = 1
}

/**
 * This portfolio only consists of setting 3, and is used to verify the reproducibility of the Allocateam experiment,
 * by running setting 3 three times. The output is then compared in a separate Python module in
 * tools/plot/verify_reproducibility.py.
 */
public class VerifyReproducibilityPortfolio(parent: Experiment, id: Int) : Portfolio(parent, id, "VerifyReproducibilityPortfolio") {
    override val topologies: List<Topology> = listOf(
        Topology("medium"),
    )

    override val workloads: List<Workload> = listOf(
        Workload("spec_trace-2", 1.0),
    )

    override val resourceAllocationPolicies: List<String> = listOf(
        "min-min",
        "max-min",
        "round-robin",
        "lottery",
        "heft",
        "elop"
    )

    override val repetitions: Int = 3
}
