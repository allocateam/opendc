package org.opendc.experiments.allocateam

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.choice
import com.github.ajalt.clikt.parameters.types.file
import com.github.ajalt.clikt.parameters.types.int
import mu.KotlinLogging
import org.opendc.experiments.allocateam.experiment.AllocateamPortfolio
import org.opendc.experiments.allocateam.experiment.Experiment
import org.opendc.experiments.allocateam.experiment.Portfolio
import org.opendc.experiments.allocateam.experiment.VerifyRepeatabilityPortfolio
import org.opendc.experiments.sc20.reporter.ConsoleExperimentReporter
import org.opendc.experiments.sc20.runner.ExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ThreadPoolExperimentScheduler
import org.opendc.experiments.sc20.runner.internal.DefaultExperimentRunner

private val logger = KotlinLogging.logger {}

public class ExperimentCli : CliktCommand(name = "allocateam") {
    /**
     * The path to the directory where the traces are located.
     */
    private val tracePath by option("--trace-path", help = "path to the traces directory")
        .file(canBeFile = false, mustExist = true)
        .required()

    /**
     * The path to the output directory.
     */
    private val output by option("-O", "--output", help = "path to the output directory")
        .file(canBeFile = false, mustBeWritable = true)
        .required()

    /**
     * The selected portfolios to run.
     */
    private val portfolios by option("--portfolio", help = "portfolio of scenarios to explore")
        .choice(
            "smokeTest" to { experiment: Experiment, i: Int -> AllocateamPortfolio(experiment, i) }
                as (Experiment, Int) -> Portfolio,
            "verifyRepeatability" to { experiment: Experiment, i: Int -> VerifyRepeatabilityPortfolio(experiment, i) }
                as (Experiment, Int) -> Portfolio,
            ignoreCase = true
        )
        .multiple(required = true)

    /**
     * The maximum number of worker threads to use.
     */
    private val parallelism by option("--parallelism", help = "maximum number of concurrent simulation runs")
        .int()
        .default(Runtime.getRuntime().availableProcessors())

    override fun run() {
        logger.info { "Creating experiment descriptor" }

        val descriptor = object :
            Experiment(tracePath, output) {
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

public fun main(args: Array<String>): Unit = ExperimentCli().main(args)
