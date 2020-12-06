package org.opendc.experiments.allocateam.experiment

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import org.opendc.experiments.allocateam.monitors.AllocateamExperimentMonitor
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.WorkflowEvent
import java.time.Clock

/**
 * The logger for the attached monitor(s).
 */
private val logger = KotlinLogging.logger {}

/**
 * Attach a monitor to the
 */
@OptIn(ExperimentalCoroutinesApi::class)
public suspend fun attachMonitor(
    coroutineScope: CoroutineScope,
    clock: Clock,
    scheduler: StageWorkflowService,
    monitor: AllocateamExperimentMonitor
) {
    var total = 0
    var finished = 0

    scheduler.events
        .onEach { event ->
            when (event) {
                is WorkflowEvent.JobStarted -> {
                    monitor.reportJobStarted(event)
                    logger.info { "Job ${event.job.uid} started" }
                }

                is WorkflowEvent.JobFinished -> {
                    monitor.reportJobFinished(clock.millis(), event)
                    finished += 1
                    logger.info { "Jobs $finished/$total finished (${event.job.tasks.size} tasks)" }
                }
            }
        }
        .launchIn(coroutineScope)
}
