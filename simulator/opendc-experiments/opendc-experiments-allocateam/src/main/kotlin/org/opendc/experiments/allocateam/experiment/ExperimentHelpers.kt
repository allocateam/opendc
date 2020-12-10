package org.opendc.experiments.allocateam.experiment

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import org.opendc.experiments.allocateam.experiment.monitor.RunMonitor
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
    monitor: RunMonitor
) {

    scheduler.events
        .onEach { event ->
            when (event) {
                is WorkflowEvent.JobStarted -> {
                    monitor.reportJobStarted(event)
                }

                is WorkflowEvent.JobFinished -> {
                    monitor.reportJobFinished(event)
                }

                is WorkflowEvent.TaskStarted -> {
                    monitor.reportTaskStarted()
                }
                is WorkflowEvent.TaskFinished -> {
                    monitor.reportTaskFinished()
                }
            }
        }
        .launchIn(coroutineScope)
}
