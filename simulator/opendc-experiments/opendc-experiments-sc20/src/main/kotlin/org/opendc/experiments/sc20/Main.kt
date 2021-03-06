/*
 * Copyright (c) 2020 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.experiments.sc20

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.defaultLazy
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.file
import com.github.ajalt.clikt.parameters.types.int
import mu.KotlinLogging
import org.opendc.experiments.sc20.experiment.*
import org.opendc.experiments.sc20.reporter.ConsoleExperimentReporter
import org.opendc.experiments.sc20.runner.ExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ThreadPoolExperimentScheduler
import org.opendc.experiments.sc20.runner.internal.DefaultExperimentRunner
import org.opendc.format.trace.sc20.Sc20PerformanceInterferenceReader
import org.opendc.format.trace.sc20.Sc20VmPlacementReader
import java.io.File

/**
 * The logger for this experiment.
 */
private val logger = KotlinLogging.logger {}

/**
 * Represents the command for running the experiment.
 */
public class ExperimentCli : CliktCommand(name = "sc20-experiment") {
    /**
     * The path to the directory where the topology descriptions are located.
     */
    private val environmentPath by option("--environment-path", help = "path to the environment directory")
        .file(canBeFile = false)
        .required()

    /**
     * The path to the directory where the traces are located.
     */
    private val tracePath by option("--trace-path", help = "path to the traces directory")
        .file(canBeFile = false)
        .required()

    /**
     * The path to the performance interference model.
     */
    private val performanceInterferenceStream by option(
        "--performance-interference-model",
        help = "path to the performance interference file"
    )
        .file(canBeDir = false)
        .convert { it.inputStream() }

    /**
     * The path to the original VM placements file.
     */
    private val vmPlacements by option("--vm-placements-file", help = "path to the VM placement file")
        .file(canBeDir = false)
        .convert {
            Sc20VmPlacementReader(it.inputStream().buffered()).construct()
        }
        .default(emptyMap())

    /**
     * The selected portfolios to run.
     */
    private val portfolios by option("--portfolio", help = "portfolio of scenarios to explore")
        .choice(
            "hor-ver" to { experiment: Experiment, i: Int -> HorVerPortfolio(experiment, i) }
                as (Experiment, Int) -> Portfolio,
            "more-velocity" to { experiment, i -> MoreVelocityPortfolio(experiment, i) },
            "composite-workload" to { experiment, i -> CompositeWorkloadPortfolio(experiment, i) },
            "operational-phenomena" to { experiment, i -> OperationalPhenomenaPortfolio(experiment, i) },
            "replay" to { experiment, i -> ReplayPortfolio(experiment, i) },
            "test" to { experiment, i -> TestPortfolio(experiment, i) },
            "more-hpc" to { experiment, i -> MoreHpcPortfolio(experiment, i) },
            ignoreCase = true
        )
        .multiple(required = true)

    /**
     * The maximum number of worker threads to use.
     */
    private val parallelism by option("--parallelism", help = "maximum number of concurrent simulation runs")
        .int()
        .default(Runtime.getRuntime().availableProcessors())

    /**
     * The buffer size for writing results.
     */
    private val bufferSize by option("--buffer-size")
        .int()
        .default(4096)

    /**
     * The path to the output directory.
     */
    private val output by option("-O", "--output", help = "path to the output directory")
        .file(canBeFile = false)
        .defaultLazy { File("data") }

    override fun run() {
        logger.info { "Constructing performance interference model" }

        val performanceInterferenceModel =
            performanceInterferenceStream?.let { Sc20PerformanceInterferenceReader(it) }

        logger.info { "Creating experiment descriptor" }
        val descriptor = object :
            Experiment(environmentPath, tracePath, output, performanceInterferenceModel, vmPlacements, bufferSize) {
            private val descriptor = this
            override val children: Sequence<ExperimentDescriptor> = sequence {
                for ((i, producer) in portfolios.withIndex()) {
                    yield(producer(descriptor, i))
                }
            }
        }

        logger.info { "Starting experiment runner [parallelism=$parallelism]" }
        val scheduler = ThreadPoolExperimentScheduler(parallelism)
        val runner = DefaultExperimentRunner(scheduler)
        val reporter = ConsoleExperimentReporter()
        try {
            runner.execute(descriptor, reporter)
        } finally {
            scheduler.close()
            reporter.close()
        }
    }
}

/**
 * Main entry point of the experiment.
 */
public fun main(args: Array<String>): Unit = ExperimentCli().main(args)
