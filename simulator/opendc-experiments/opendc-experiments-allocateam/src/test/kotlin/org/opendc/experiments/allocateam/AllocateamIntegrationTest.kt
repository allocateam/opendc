package org.opendc.experiments.allocateam

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScope
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.opendc.compute.core.metal.Node
import org.opendc.compute.core.metal.service.ProvisioningService
import org.opendc.compute.simulator.SimWorkloadImage
import org.opendc.core.User
import org.opendc.experiments.allocateam.policies.ELOPJobAdmissionPolicy
import org.opendc.experiments.allocateam.policies.ELOPResourceSelectionPolicy
import org.opendc.format.environment.sc18.Sc18EnvironmentReader
import org.opendc.simulator.compute.workload.SimFlopsWorkload
import org.opendc.simulator.utils.DelayControllerClockAdapter
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.WorkflowSchedulerMode
import org.opendc.workflows.service.stage.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflows.service.stage.resource.FunctionalResourceFilterPolicy
import org.opendc.workflows.service.stage.task.NullTaskEligibilityPolicy
import org.opendc.workflows.service.stage.task.SubmissionTimeTaskOrderPolicy
import org.opendc.workflows.workload.Job
import org.opendc.workflows.workload.Task
import org.opendc.workflows.workload.WORKFLOW_TASK_DEADLINE
import java.io.File
import java.time.Clock
import java.util.*

@OptIn(ExperimentalCoroutinesApi::class)
class AllocateamIntegrationTest {
    /**
     * The [TestCoroutineScope] to use.
     */
    private lateinit var testScope: TestCoroutineScope

    /**
     * The simulation clock to use.
     */
    private lateinit var clock: Clock

    /**
     * The monitor used to keep track of the metrics.
     */
//    private lateinit var monitor: TestExperimentReporter

    /**
     * Setup the experimental environment.
     */
    @BeforeEach
    fun setUp() {
        testScope = TestCoroutineScope()
        clock = DelayControllerClockAdapter(testScope)
//        monitor = RunMonitor(this, clock)
    }

    /**
     * Tear down the experimental environment.
     */
    @AfterEach
    fun tearDown() = testScope.cleanupTestCoroutines()

    @Test
    fun testELOP() {
        val schedulerAsync = testScope.async {
            // Environment file describing topology can be found in the resources of this project
            val resourcesFile = File("/env/", "single.json").absolutePath
            val environment = Sc18EnvironmentReader(object {}.javaClass.getResourceAsStream(resourcesFile))
                .use { it.construct(testScope, clock) }

            val elopReservedNodes: MutableMap<JobState, MutableList<Node>> = mutableMapOf()
            StageWorkflowService(
                testScope,
                clock,
                environment.platforms[0].zones[0].services[ProvisioningService],
                mode = WorkflowSchedulerMode.Batch(100),
                jobAdmissionPolicy = ELOPJobAdmissionPolicy(elopReservedNodes),
                jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
                taskEligibilityPolicy = NullTaskEligibilityPolicy,
                taskOrderPolicy = SubmissionTimeTaskOrderPolicy(),
                resourceFilterPolicy = FunctionalResourceFilterPolicy,
                resourceSelectionPolicy = ELOPResourceSelectionPolicy(elopReservedNodes),
            )
        }

        testScope.launch {
            val scheduler = schedulerAsync.await()

            val jobs = listOf(createJob(), createJob(), createJob())

            for (job in jobs) {
                scheduler.submit(job)
            }
        }
        try {
            testScope.advanceUntilIdle()
        } finally { }
    }

    private fun createJob(): Job {
        fun createTasks(numberOfTasks: Int): MutableMap<Int, Task> {
            val tasks = mutableMapOf<Int, Task>()
            val runtime = (100).toLong()
            val cores = 1
            val flops: Long = 4100 * (runtime / 1000) * cores
            for (i in 1..numberOfTasks) {
                tasks[i] = Task(
                    UUID.randomUUID(),
                    "<unnamed>",
                    SimWorkloadImage(UUID.randomUUID(), "<unnamed>", emptyMap(), SimFlopsWorkload(flops, cores)),
                    HashSet(),
                    mapOf(WORKFLOW_TASK_DEADLINE to runtime)
                )
            }
            return tasks
        }

        fun setDependencies(
            tasks: MutableMap<Int, Task>,
            taskDependencies: MutableMap<Int, List<Int>>
        ): MutableMap<Int, Task> {
            for ((id, task) in tasks) {
                val dependencies = taskDependencies[id]
                if (dependencies != null) {
                    for (dependencyId in dependencies) {
                        val dependency = tasks[dependencyId]!!
                        (task.dependencies as MutableSet<Task>).add(dependency)
                        tasks[id] = task
                    }
                }
            }

            return tasks
        }

        var tasks = createTasks(4)
        val taskDependencies = mutableMapOf<Int, List<Int>>()
        taskDependencies[2] = listOf(1)
        taskDependencies[3] = listOf(1)
        taskDependencies[4] = listOf(2, 3)
        tasks = setDependencies(tasks, taskDependencies)
        return Job(UUID.randomUUID(), "<unnamed>", UnnamedUser, tasks.values.toSet())
    }

    @Test
    fun testLopCalculator() {

//        val taskState = TaskState(jobInstance, tasks[1]!!)
//
//        val lopCalculator = LevelOfParallelismCalculator()
//        println(lopCalculator.calculateLOP(taskState))
//        assert(lopCalculator.calculateLOP(taskState) == 2)
    }


}

/**
 * An unnamed user.
 */
private object UnnamedUser : User {
    override val name: String = "<unnamed>"
    override val uid: UUID = UUID.randomUUID()
}
