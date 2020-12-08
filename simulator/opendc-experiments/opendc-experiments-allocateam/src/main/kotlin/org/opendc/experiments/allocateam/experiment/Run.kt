package org.opendc.experiments.allocateam.experiment

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScope
import mu.KotlinLogging
import org.opendc.compute.core.metal.service.ProvisioningService
import org.opendc.experiments.allocateam.experiment.monitor.RunMonitor
import org.opendc.experiments.allocateam.policies.MinMaxResourceSelectionPolicy
import org.opendc.experiments.sc20.runner.TrialExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionContext
import org.opendc.format.environment.sc18.Sc18EnvironmentReader
import org.opendc.format.trace.wtf.WtfTraceReader
import org.opendc.simulator.utils.DelayControllerClockAdapter
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.WorkflowSchedulerMode
import org.opendc.workflows.service.stage.job.NullJobAdmissionPolicy
import org.opendc.workflows.service.stage.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflows.service.stage.resource.FirstFitResourceSelectionPolicy
import org.opendc.workflows.service.stage.resource.FunctionalResourceFilterPolicy
import org.opendc.workflows.service.stage.task.NullTaskEligibilityPolicy
import org.opendc.workflows.service.stage.task.SubmissionTimeTaskOrderPolicy
import java.io.File
import kotlin.math.max

/**
 * The logger for the experiment scenario.
 */
private val logger = KotlinLogging.logger {}

/**
 * A repetition of a scenario that runs the scenario on the simulator.
 */
public data class Run(override val parent: Scenario, val id: Int, val seed: Int) : TrialExperimentDescriptor() {
    public var runStatus: RunStatus = RunStatus.IDLE

    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun invoke(context: ExperimentExecutionContext) {
        val experiment = parent.parent.parent
        val testScope = TestCoroutineScope()
        val clock = DelayControllerClockAdapter(testScope)

        val monitor = RunMonitor(this, clock)

        val allocationPolicy = when (parent.allocationPolicy) {
            "first-fit" -> FirstFitResourceSelectionPolicy
            "min-max" -> MinMaxResourceSelectionPolicy
            else -> throw IllegalArgumentException("Unknown policy ${parent.allocationPolicy}")
        }

        val schedulerAsync = testScope.async {
            // Environment file describing topology can be found in the resources of this project
            var resourcesFile = File("/env/", parent.topology.name + ".json").absolutePath
            val environment = Sc18EnvironmentReader(object {}.javaClass.getResourceAsStream(resourcesFile))
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
                resourceSelectionPolicy = allocationPolicy,
            )
        }

        // attach monitor to scheduler
        testScope.launch {
            val scheduler = schedulerAsync.await()
            attachMonitor(
                this,
                clock,
                scheduler,
                monitor
            )
        }

        this.runStatus = RunStatus.RUNNING

        testScope.launch {
            val tracePath = File(experiment.traces.absolutePath, parent.workload.name).absolutePath
            val reader = WtfTraceReader(tracePath)
            val scheduler = schedulerAsync.await()
            while (reader.hasNext()) {
                val (time, job) = reader.next()
                delay(max(0, time * 1000 - clock.millis()))
                scheduler.submit(job)
            }
        }

        try {
            testScope.advanceUntilIdle()
        } finally {
            this.runStatus = RunStatus.FINISHED
            monitor.generateMetrics()
        }
    }
}
