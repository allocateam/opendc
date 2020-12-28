package org.opendc.experiments.allocateam.experiment

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScope
import mu.KotlinLogging
import org.opendc.compute.core.metal.service.ProvisioningService
import org.opendc.compute.core.metal.service.SimpleProvisioningService
import org.opendc.experiments.allocateam.environment.AllocateamEnvironmentReader
import org.opendc.experiments.allocateam.experiment.monitor.ParquetExperimentMonitor
import org.opendc.experiments.allocateam.policies.*
import org.opendc.experiments.allocateam.policies.heft.HeftPolicyState
import org.opendc.experiments.allocateam.policies.heft.HeftResourceSelectionPolicy
import org.opendc.experiments.allocateam.policies.heft.HeftTaskOrderPolicy
import org.opendc.experiments.sc20.runner.TrialExperimentDescriptor
import org.opendc.experiments.sc20.runner.execution.ExperimentExecutionContext
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
    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun invoke(context: ExperimentExecutionContext) {
        val experiment = parent.parent.parent
        val testScope = TestCoroutineScope()
        val clock = DelayControllerClockAdapter(testScope)

        val flopsPerCore = 1105000000000  // based on FLOPS of i7 6700k per core

        val heftPolicyState = HeftPolicyState()

        val resourceSelectionPolicy = when (parent.allocationPolicy) {
            "min-min" -> MinMinResourceSelectionPolicy(flopsPerCore)
            "max-min" -> MaxMinResourceSelectionPolicy(flopsPerCore)
            "heft" -> HeftResourceSelectionPolicy(heftPolicyState)
            "round-robin" -> FirstFitResourceSelectionPolicy
            "lottery" -> FirstFitResourceSelectionPolicy
            else -> throw IllegalArgumentException("Unknown policy ${parent.allocationPolicy}")
        }

        val taskEligibilityPolicy = when (parent.allocationPolicy) {
            "round-robin" -> RoundRobinPolicy(30)
            else -> NullTaskEligibilityPolicy
        }

        val taskOrderPolicy = when(parent.allocationPolicy) {
            "lottery" -> LotteryPolicy(50)
            "heft" -> HeftTaskOrderPolicy(heftPolicyState)
            else -> SubmissionTimeTaskOrderPolicy()
        }

        val monitor = ParquetExperimentMonitor(
            parent.parent.parent.output,
            "portfolio_id=${parent.parent.id}/scenario_id=${parent.id}/run_id=$id",
            4096
        )

        // Environment file describing topology can be found in the resources of this project
        val resourcesFile = File("/env/", parent.topology.name + ".json").absolutePath
        val environment = AllocateamEnvironmentReader(object {}.javaClass.getResourceAsStream(resourcesFile))
            .use { it.construct(testScope, clock) }

        val provisioningService = environment.platforms[0].zones[0].services[ProvisioningService] as SimpleProvisioningService

        val schedulerAsync = testScope.async {
            StageWorkflowService(
                testScope,
                clock,
                provisioningService,
                mode = WorkflowSchedulerMode.Batch(100),

                // Admit all jobs
                jobAdmissionPolicy = NullJobAdmissionPolicy,

                // Order jobs by their submission time
                jobOrderPolicy = SubmissionTimeJobOrderPolicy(),

                // All tasks are eligible to be scheduled
                taskEligibilityPolicy = taskEligibilityPolicy,

                // Order tasks by their submission time
                taskOrderPolicy = taskOrderPolicy,

                // Put tasks on resources that can actually run them
                resourceFilterPolicy = FunctionalResourceFilterPolicy,

                // Actual allocation policy (select the resource on which to place the task)
                resourceSelectionPolicy = resourceSelectionPolicy,
            )
        }

        // attach monitor to scheduler
        testScope.launch {
            val scheduler = schedulerAsync.await()
            attachMonitor(
                this,
                clock,
                scheduler,
                monitor,
                provisioningService
            )
        }

        testScope.launch {
            val tracePath = File(experiment.traces.absolutePath, parent.workload.name).absolutePath
            val reader = WtfTraceReader(tracePath)
            val scheduler = schedulerAsync.await()

            monitor.reportRunStarted(clock.millis())
            while (reader.hasNext()) {
                val (time, job) = reader.next()
                delay(max(0, time * 1000 - clock.millis()))
                scheduler.submit(job)
            }
        }

        try {
            testScope.advanceUntilIdle()
            testScope.uncaughtExceptions.forEach { it.printStackTrace() }
        } finally {
            monitor.reportRunFinished(clock.millis())
            monitor.close()
        }
    }
}
