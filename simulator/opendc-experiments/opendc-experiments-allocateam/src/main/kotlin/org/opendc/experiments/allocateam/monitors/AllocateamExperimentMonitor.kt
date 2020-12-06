package org.opendc.experiments.allocateam.monitors

import org.opendc.compute.core.Server
import org.opendc.experiments.allocateam.telemetry.JobFinishedEvent
import org.opendc.experiments.allocateam.telemetry.ParquetJobEventWriter
import org.opendc.experiments.sc20.telemetry.HostEvent
import org.opendc.workflows.service.WorkflowEvent
import org.opendc.workflows.workload.Job
import java.io.File

public class AllocateamExperimentMonitor(base: File, partition: String, bufferSize: Int) {
    private val jobWriter = ParquetJobEventWriter(
        File(base, "job-metrics/$partition/data.parquet"),
        bufferSize
    )

    private val startTimesPerJob = mutableMapOf<Job, Long>()

    public fun reportJobStarted(event: WorkflowEvent.JobStarted) {
        startTimesPerJob[event.job] = event.time
    }

    public fun reportJobFinished(time: Long, event: WorkflowEvent.JobFinished) {
        val startTime = startTimesPerJob[event.job]!!
        val duration = event.time - startTime
        jobWriter.write(JobFinishedEvent(time, duration))
        startTimesPerJob.remove(event.job)
    }

    public fun close() {

        jobWriter.close()
    }
}
