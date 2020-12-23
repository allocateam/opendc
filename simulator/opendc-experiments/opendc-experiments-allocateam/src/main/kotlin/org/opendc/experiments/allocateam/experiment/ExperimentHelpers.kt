package org.opendc.experiments.allocateam.experiment

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import org.opendc.compute.core.metal.service.SimpleProvisioningService
import org.opendc.compute.simulator.SimBareMetalDriver
import org.opendc.experiments.allocateam.experiment.monitor.ParquetExperimentMonitor
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.WorkflowEvent
import java.time.Clock

/**
 * The logger for the attached monitor(s).
 */
private val logger = KotlinLogging.logger {}

/**
 * Attach the monitor to the scheduler.
 */
@OptIn(ExperimentalCoroutinesApi::class)
public suspend fun attachMonitor(
    coroutineScope: CoroutineScope,
    clock: Clock,
    scheduler: StageWorkflowService,
    monitor: ParquetExperimentMonitor,
    provisioningService: SimpleProvisioningService
) {
    scheduler.events
        .onEach { event ->
            val time = clock.millis()
            when (event) {
                is WorkflowEvent.JobStarted -> {
                    monitor.reportJobStarted(event)
                }

                is WorkflowEvent.JobFinished -> {
                    monitor.reportJobFinished(clock.millis(), event)
                }

                is WorkflowEvent.TaskStarted -> {
                    monitor.reportTaskStarted(time, event.task)
                }

                is WorkflowEvent.TaskFinished -> {
                    monitor.reportTaskFinished(time, event.task)
                }
            }
        }
        .launchIn(coroutineScope)

    for ((node, driver) in provisioningService.allNodes()) {
        val powerModel = (driver as SimBareMetalDriver).powerDraw
        powerModel
            .onEach { wattage ->
                val time = clock.millis()
                monitor.reportPowerConsumption(time, node, wattage)
            }
            .launchIn(coroutineScope)
    }
}
