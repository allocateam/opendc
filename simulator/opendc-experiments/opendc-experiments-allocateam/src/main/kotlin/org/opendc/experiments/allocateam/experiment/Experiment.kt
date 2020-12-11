package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.allocateam.telemetry.events.RunCompletedEvent
import org.opendc.experiments.allocateam.telemetry.writers.RunCompletedEventWriter
import org.opendc.experiments.sc20.runner.ContainerExperimentDescriptor
import org.opendc.experiments.sc20.runner.ExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionContext
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionListener
import java.io.File

/**
 * The global configuration of the experiment. Is also responsible for accumulating metrics for each run and
 * writing them to a file.
 *
 * @param traces The path to the traces directory.
 * @param output The output directory.
 */
public abstract class Experiment(
    public val traces: File,
    public val output: File
) : ContainerExperimentDescriptor() {
    override val parent: ExperimentDescriptor? = null

    override suspend fun invoke(context: ExperimentExecutionContext) {
        val writer = RunCompletedEventWriter(File(output, "experiments.parquet"))
        try {
            val listener = object : ExperimentExecutionListener by context.listener {
                override fun descriptorRegistered(descriptor: ExperimentDescriptor) {
                    if (descriptor is Run) {
                        writer.write(RunCompletedEvent(descriptor, System.currentTimeMillis()))
                    }
                        context.listener.descriptorRegistered(descriptor)
                }
            }

            val newContext = object : ExperimentExecutionContext by context {
                override val listener: ExperimentExecutionListener = listener
            }

            super.invoke(newContext)
        } finally {
            writer.close()
        }
    }
}
