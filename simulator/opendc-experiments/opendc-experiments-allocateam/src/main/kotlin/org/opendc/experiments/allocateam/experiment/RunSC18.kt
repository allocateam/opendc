package org.opendc.experiments.allocateam.experiment

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.test.TestCoroutineScope
import mu.KotlinLogging
import org.opendc.compute.core.metal.service.ProvisioningService
import org.opendc.experiments.sc20.runner.TrialExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionContext
import org.opendc.format.environment.sc18.Sc18EnvironmentReader
import org.opendc.format.trace.wtf.WtfTraceReader
import org.opendc.simulator.utils.DelayControllerClockAdapter
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.WorkflowEvent
import org.opendc.workflows.service.WorkflowSchedulerMode
import org.opendc.workflows.service.stage.job.NullJobAdmissionPolicy
import org.opendc.workflows.service.stage.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflows.service.stage.resource.FirstFitResourceSelectionPolicy
import org.opendc.workflows.service.stage.resource.FunctionalResourceFilterPolicy
import org.opendc.workflows.service.stage.task.NullTaskEligibilityPolicy
import org.opendc.workflows.service.stage.task.SubmissionTimeTaskOrderPolicy
import kotlin.math.max

/**
 * The logger for the experiment scenario.
 */
private val logger = KotlinLogging.logger {}

public data class RunSC18(override val parent: Scenario, val id: Int, val seed: Int) : TrialExperimentDescriptor() {
    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun invoke(context: ExperimentExecutionContext) {
        val experiment = parent.parent.parent
        val testScope = TestCoroutineScope()
        val clock = DelayControllerClockAdapter(testScope)

        var total = 0
        var finished = 0

        val schedulerAsync = testScope.async {
            // TODO(gm): replace the environment format/reader with one that makes more sense in the context of this project
            // Environment file can be found in the `environments` folder under the topology name
            val environment = Sc18EnvironmentReader(object {}.javaClass.getResourceAsStream("/env/setup-test.json"))
                .use { it.construct(testScope, clock) }

            StageWorkflowService(
                testScope,
                clock,
                environment.platforms[0].zones[0].services[ProvisioningService],
                mode = WorkflowSchedulerMode.Batch(100),

                // Admit all jobs
                jobAdmissionPolicy = NullJobAdmissionPolicy,

                // Order jobs by their submission time
                jobOrderPolicy = SubmissionTimeJobOrderPolicy(),

                // All tasks are eligible to be scheduled
                taskEligibilityPolicy = NullTaskEligibilityPolicy,

                // Order tasks by their submission time
                taskOrderPolicy = SubmissionTimeTaskOrderPolicy(),

                // Put tasks on resources that can actually run them
                resourceFilterPolicy = FunctionalResourceFilterPolicy,

                // Actual allocation policy (select the resource on which to place the task)
                resourceSelectionPolicy = FirstFitResourceSelectionPolicy,
            )
        }

        testScope.launch {
            val scheduler = schedulerAsync.await()
            scheduler.events
                .onEach { event ->
                    when (event) {
                        is WorkflowEvent.JobStarted -> {
                            logger.info { "Job ${event.job.uid} started" }
                        }
                        is WorkflowEvent.JobFinished -> {
                            finished += 1
                            logger.info { "Jobs $finished/$total finished (${event.job.tasks.size} tasks)" }
                        }
                    }
                }
                .collect()
        }


        testScope.launch {
            val reader = WtfTraceReader(experiment.traces.absolutePath)
            val scheduler = schedulerAsync.await()

            while (reader.hasNext()) {
                val (time, job) = reader.next()
                total += 1
                delay(max(0, time * 1000 - clock.millis()))
                scheduler.submit(job)
            }
        }

        testScope.advanceUntilIdle()
    }
}
