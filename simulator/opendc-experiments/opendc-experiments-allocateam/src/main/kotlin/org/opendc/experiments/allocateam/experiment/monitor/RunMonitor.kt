package org.opendc.experiments.allocateam.experiment.monitor

import mu.KotlinLogging
import org.opendc.experiments.allocateam.experiment.Run
import org.opendc.experiments.allocateam.experiment.RunStatus
import org.opendc.experiments.allocateam.telemetry.RunMetrics
import org.opendc.experiments.allocateam.telemetry.events.RunCompletedEvent
import org.opendc.workflows.service.WorkflowEvent
import org.opendc.workflows.workload.Job
import java.time.Clock
import java.util.concurrent.TimeUnit

/**
 * The logger for the attached monitor(s).
 */
private val logger = KotlinLogging.logger {}


/**
 * Monitor certain events within a run in order to produce metrics for the run.
 *
 */
public class RunMonitor(private val run: Run, private val clock: Clock) {
    // (intermediate) metric aggregators
    private val startTime: Long = System.currentTimeMillis()
    private var startedJobs = 0
    private var finishedJobs = 0
    private var startedTasks = 0
    private var finishedTasks = 0

    private val startTimesPerJob = mutableMapOf<Job, Long>()

    /**
     * Generate all metrics from the intermediate metric aggregators
     */
    public fun generateMetrics() {
        // Metrics
        val taskThroughput = this.calculateTaskThroughput()

        val runMetrics = RunMetrics(taskThroughput)
        val runCompletedEvent = RunCompletedEvent(run, runMetrics, clock.millis())
        val experiment = run.parent.parent.parent
        experiment.storeRunCompletedEvent(runCompletedEvent)
    }

    private fun calculateTaskThroughput(): Double {
        val runDuration = this.getRunDuration()
        return (finishedTasks / runDuration).toDouble()
    }

    private fun getRunDuration(): Long {
        if (this.run.runStatus != RunStatus.FINISHED) {
            throw Exception("Attempting to obtain run duration while run is not finished.")
        }
        val currentTime = System.currentTimeMillis()
        val runDurationInMillis = (currentTime - startTime)
        val runDuration = TimeUnit.MILLISECONDS.toSeconds(runDurationInMillis);
        return runDuration
    }

    public fun reportJobStarted(event: WorkflowEvent.JobStarted) {
        startTimesPerJob[event.job] = event.time
        this.startedJobs += 1
//        logger.info { "Job ${event.job.uid} started" }
    }

    public fun reportJobSubmitted(event: WorkflowEvent.JobStarted) {
        startTimesPerJob[event.job] = event.time
    }

    public fun reportJobFinished(time: Long, event: WorkflowEvent.JobFinished) {
        val startTime = startTimesPerJob[event.job]!!
        val duration = event.time - startTime
        startTimesPerJob.remove(event.job)
        this.finishedJobs += 1
//        logger.info { "$finishedJobs jobs finished of ${this.startedJobs} started and completed " +
//            "jobs (${event.job.tasks.size} tasks)" }
    }

    public fun reportTaskStarted() {
        this.startedTasks += 1
    }

    public fun reportTaskFinished() {
        this.finishedTasks += 1
    }
}
