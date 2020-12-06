package org.opendc.experiments.allocateam.experiment

import org.opendc.experiments.allocateam.telemetry.ParquetRunEventWriter
import org.opendc.experiments.allocateam.telemetry.RunEvent
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
 * @param vmPlacements Original VM placement in the trace.
 * @param bufferSize The buffer size of the event reporters.
 */
public abstract class Experiment(
    public val traces: File,
    public val output: File,
    public val vmPlacements: Map<String, String>,
    public val bufferSize: Int
) : ContainerExperimentDescriptor() {
    override val parent: ExperimentDescriptor? = null

    override suspend fun invoke(context: ExperimentExecutionContext) {
        val writer = ParquetRunEventWriter(File(output, "experiments.parquet"), bufferSize)
        try {
            val listener = object : ExperimentExecutionListener by context.listener {
                override fun descriptorRegistered(descriptor: ExperimentDescriptor) {
                    if (descriptor is Run) {
                        writer.write(RunEvent(descriptor, System.currentTimeMillis()))
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
