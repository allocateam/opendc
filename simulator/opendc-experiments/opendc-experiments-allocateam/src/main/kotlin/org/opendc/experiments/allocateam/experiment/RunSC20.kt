package org.opendc.experiments.allocateam.experiment

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScope
import mu.KotlinLogging
import org.opendc.compute.simulator.allocation.*
import org.opendc.experiments.sc20.experiment.attachMonitor
import org.opendc.experiments.sc20.experiment.createProvisioner
import org.opendc.experiments.sc20.experiment.model.Workload
import org.opendc.experiments.sc20.experiment.processTrace
import org.opendc.experiments.sc20.runner.TrialExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionContext
import org.opendc.experiments.sc20.trace.Sc20ParquetTraceReader
import org.opendc.experiments.sc20.trace.Sc20RawParquetTraceReader
import org.opendc.format.environment.sc18.Sc18EnvironmentReader
import org.opendc.simulator.utils.DelayControllerClockAdapter
import java.io.File
import kotlin.random.Random

/**
 * The logger for the experiment scenario.
 */
private val logger = KotlinLogging.logger {}

public data class RunSC20(override val parent: Scenario, val id: Int, val seed: Int) : TrialExperimentDescriptor() {
    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun invoke(context: ExperimentExecutionContext) {
        val experiment = parent.parent.parent
        val testScope = TestCoroutineScope()
        val clock = DelayControllerClockAdapter(testScope)
        val seeder = Random(seed)
        val environment = Sc18EnvironmentReader(object {}.javaClass.getResourceAsStream("/env/setup-test.json"))
        val monitor = WebExperimentMonitor()

        val chan = Channel<Unit>(Channel.CONFLATED)

        val allocationPolicy = when (parent.allocationPolicy) {
            "mem" -> AvailableMemoryAllocationPolicy()
            "mem-inv" -> AvailableMemoryAllocationPolicy(true)
            "core-mem" -> AvailableCoreMemoryAllocationPolicy()
            "core-mem-inv" -> AvailableCoreMemoryAllocationPolicy(true)
            "active-servers" -> NumberOfActiveServersAllocationPolicy()
            "active-servers-inv" -> NumberOfActiveServersAllocationPolicy(true)
            "provisioned-cores" -> ProvisionedCoresAllocationPolicy()
            "provisioned-cores-inv" -> ProvisionedCoresAllocationPolicy(true)
            "random" -> RandomAllocationPolicy(Random(seeder.nextInt()))
            "replay" -> ReplayAllocationPolicy(experiment.vmPlacements)
            else -> throw IllegalArgumentException("Unknown policy ${parent.allocationPolicy}")
        }

        val traceDir = File(experiment.traces, parent.workload.name)
        val traceReader = Sc20RawParquetTraceReader(traceDir)

        val trace = Sc20ParquetTraceReader(
            listOf(traceReader),
            emptyMap(),
            Workload(parent.workload.name, 1.0),
            seed
        )

        testScope.launch {
            val (_, scheduler) = createProvisioner(
                this,
                clock,
                environment,
                allocationPolicy
            )

            attachMonitor(this, clock, scheduler, monitor)
            processTrace(
                this,
                clock,
                trace,
                scheduler,
                chan,
                monitor
            )

            logger.debug { "SUBMIT=${scheduler.submittedVms}" }
            logger.debug { "FAIL=${scheduler.unscheduledVms}" }
            logger.debug { "QUEUED=${scheduler.queuedVms}" }
            logger.debug { "RUNNING=${scheduler.runningVms}" }
            logger.debug { "FINISHED=${scheduler.finishedVms}" }

            scheduler.terminate()
        }

        try {
            testScope.advanceUntilIdle()
        } finally {
            monitor.close()
        }

        println(monitor.getResult())

        // TODO(gm): write these metrics to a file (include repeat number in filename)
    }
}
