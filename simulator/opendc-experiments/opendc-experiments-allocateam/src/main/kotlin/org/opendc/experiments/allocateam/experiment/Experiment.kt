package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.sc20.runner.ContainerExperimentDescriptor
import org.opendc.experiments.sc20.runner.ExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionContext
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionListener
import java.io.File

/**
 * The global configuration of the experiment.
 *
 * @param traces The path to the traces directory.
 * @param output The output directory.
 */
public abstract class Experiment(public val traces: File, public val output: File) : ContainerExperimentDescriptor() {
    override val parent: ExperimentDescriptor? = null

    override suspend fun invoke(context: ExperimentExecutionContext) {
        val listener = object : ExperimentExecutionListener by context.listener {
            override fun descriptorRegistered(descriptor: ExperimentDescriptor) {
                context.listener.descriptorRegistered(descriptor)
            }
        }

        val newContext = object : ExperimentExecutionContext by context {
            override val listener: ExperimentExecutionListener = listener
        }

        super.invoke(newContext)
    }
}
