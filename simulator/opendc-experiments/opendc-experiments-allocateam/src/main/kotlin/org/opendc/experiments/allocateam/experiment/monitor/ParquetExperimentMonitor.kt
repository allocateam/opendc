package org.opendc.experiments.allocateam.experiment.monitor

import mu.KotlinLogging
import org.opendc.experiments.allocateam.telemetry.events.TurnaroundTimeEvent
import org.opendc.experiments.allocateam.telemetry.events.TaskThroughputEvent
import org.opendc.experiments.allocateam.telemetry.writers.TaskThroughputWriter
import org.opendc.experiments.allocateam.telemetry.writers.TurnaroundTimeWriter
import org.opendc.workflows.service.WorkflowEvent
import org.opendc.workflows.workload.Job
import java.io.File

private val logger = KotlinLogging.logger {}

public class ParquetExperimentMonitor(base: File, partition: String, bufferSize: Int) {
    private var startTime: Long = 0
    private var finishedTasks = 0

    private val submissionTimesPerJob = mutableMapOf<Job, Long>()

    private val turnaroundTimeWriter = TurnaroundTimeWriter(
        File(base, "turnaround-time/$partition/data.parquet"),
        bufferSize
    )

    private val taskThroughputWriter = TaskThroughputWriter(
        File(base, "task-throughput/$partition/data.parquet"),
        bufferSize
    )

    public fun reportRunStarted(time: Long) {
        startTime = time
    }

    public fun reportRunFinished(time: Long) {
        val runDuration = time - startTime

        taskThroughputWriter.write(
            TaskThroughputEvent(
                time,
                finishedTasks / runDuration.toDouble()
            )
        )
    }

    public fun reportTaskFinished() {
        this.finishedTasks += 1
    }

    public fun reportJobStarted(event: WorkflowEvent.JobStarted) {
        // FIXME(gm): create a proper WorkflowEvent for this
        submissionTimesPerJob[event.jobState.job] = event.jobState.submittedAt
    }

    public fun reportJobFinished(time: Long, event: WorkflowEvent.JobFinished) {
        turnaroundTimeWriter.write(
            TurnaroundTimeEvent(
                time,
                event.time - submissionTimesPerJob[event.job]!!
            )
        )

        submissionTimesPerJob.remove(event.job)
    }

    public fun close() {
        turnaroundTimeWriter.close()
        taskThroughputWriter.close()
    }
}
