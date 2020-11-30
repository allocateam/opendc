package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.runner.TrialExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionContext

public data class Run(override val parent: Scenario, val id: Int, val seed: Int) : TrialExperimentDescriptor() {
    override suspend fun invoke(context: ExperimentExecutionContext) {

        println("Nothing to see here (yet)!")

    }
}
