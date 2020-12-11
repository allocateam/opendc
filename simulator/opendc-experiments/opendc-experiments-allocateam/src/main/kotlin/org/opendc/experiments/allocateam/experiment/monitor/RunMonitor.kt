package org.opendc.experiments.allocateam.experiment.monitor

import mu.KotlinLogging
import org.opendc.experiments.allocateam.experiment.Run
import org.opendc.experiments.allocateam.experiment.RunStatus
import org.opendc.experiments.allocateam.telemetry.RunMetrics
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
    private val submissionTimesPerJob = mutableMapOf<Job, Long>()
    private val turnaroundTimes = mutableListOf<Long>()

    /**
     * Generate all metrics from the intermediate metric aggregators
     */
    public fun generateMetrics() {
        val taskThroughput = this.calculateTaskThroughput()
        val turnaroundTime = this.calculateTurnaroundTime()

        val runMetrics = RunMetrics(taskThroughput, turnaroundTime)
//        val runCompletedEvent = RunCompletedEvent(run, runMetrics, clock.millis())
//        val experiment = run.parent.parent.parent
//        experiment.storeRunCompletedEvent(runCompletedEvent)
    }

    private fun calculateTaskThroughput(): Double {
        val runDuration = this.getRunDuration()
        return (finishedTasks / runDuration).toDouble()
    }

    private fun calculateTurnaroundTime(): Double {
        return (turnaroundTimes.sum() / turnaroundTimes.count()).toDouble()
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
        startTimesPerJob[event.jobState.job] = event.time
        this.startedJobs += 1

        // FIXME(gm): create a proper WorkflowEvent for this
        submissionTimesPerJob[event.jobState.job] = event.jobState.submittedAt
    }

    public fun reportJobFinished(event: WorkflowEvent.JobFinished) {
        turnaroundTimes.add(
            event.time - submissionTimesPerJob[event.job]!!
        )

        startTimesPerJob.remove(event.job)
        submissionTimesPerJob.remove(event.job)
        this.finishedJobs += 1
    }

    public fun reportTaskStarted() {
        this.startedTasks += 1
    }

    public fun reportTaskFinished() {
        this.finishedTasks += 1
    }
}
