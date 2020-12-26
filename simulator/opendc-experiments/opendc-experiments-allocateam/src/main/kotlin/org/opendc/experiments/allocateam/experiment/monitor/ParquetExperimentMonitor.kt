package org.opendc.experiments.allocateam.experiment.monitor

import org.opendc.compute.core.metal.Node
import org.opendc.compute.simulator.SimWorkloadImage
import org.opendc.experiments.allocateam.telemetry.events.*
import org.opendc.experiments.allocateam.telemetry.writers.*
import org.opendc.simulator.compute.workload.SimFlopsWorkload
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.WorkflowEvent
import org.opendc.workflows.workload.Job
import org.opendc.workflows.workload.Task
import java.io.File

public class ParquetExperimentMonitor(base: File, partition: String, bufferSize: Int) {
    private var startTime: Long = 0

    private val jobLifecycleEvents = mutableMapOf<Job, JobLifecycleEvent>()
    private val taskLifecycleEvents = mutableMapOf<Task, TaskLifecycleEvent>()

    private val powerConsumptionWriter = PowerConsumptionWriter(
        File(base, "power-consumption/$partition/data.parquet"),
        bufferSize
    )

    private val runDurationWriter = RunDurationWriter(
        File(base, "run-duration/$partition/data.parquet"),
        bufferSize
    )

    private val jobLifecycleWriter = JobLifecycleWriter(
        File(base, "job-lifecycle/$partition/data.parquet"),
        bufferSize
    )

    private val taskLifecycleWriter = TaskLifecycleWriter(
        File(base, "task-lifecycle/$partition/data.parquet"),
        bufferSize
    )

    public fun reportRunStarted(time: Long) {
        startTime = time
    }

    public fun reportRunFinished(time: Long) {
        runDurationWriter.write(
            RunDurationEvent(
                0,
                time - startTime
            )
        )
    }

    public fun reportJobSubmitted(time: Long, event: WorkflowEvent.JobSubmitted) {
        val j = JobLifecycleEvent(0, event.jobState.job.uid.toString())
        j.submissionTime = time
        jobLifecycleEvents[event.jobState.job] = j
    }

    public fun reportJobStarted(time: Long, event: WorkflowEvent.JobStarted) {
        jobLifecycleEvents[event.jobState.job]?.startTime = time
    }

    public fun reportJobFinished(time: Long, event: WorkflowEvent.JobFinished) {
        jobLifecycleEvents[event.job]?.let {
            it.finishTime = time
            jobLifecycleWriter.write(it)
        }
    }

    public fun reportTaskSubmitted(time: Long, event: WorkflowEvent.TaskSubmitted) {
        val taskEvent = TaskLifecycleEvent(0, event.task.task.uid.toString(), event.task.job.job.uid.toString())
        taskEvent.submissionTime = time
        taskLifecycleEvents[event.task.task] = taskEvent
    }

    public fun reportTaskStarted(time: Long, task: TaskState) {
        taskLifecycleEvents[task.task]?.let {
            it.startTime = time
            it.serverID = task.host?.uid.toString()
        }
    }

    public fun reportTaskFinished(time: Long, task: TaskState) {
        taskLifecycleEvents[task.task]?.let {
            it.finishTime = time
            taskLifecycleWriter.write(it)
        }
    }

    public fun reportPowerConsumption(time: Long, host: Node, draw: Double) {
        powerConsumptionWriter.write(
            PowerConsumptionEvent(
                time,
                host.name,
                draw
            )
        )
    }

    public fun close() {
        powerConsumptionWriter.close()
        jobLifecycleWriter.close()
        taskLifecycleWriter.close()
        runDurationWriter.close()
    }
}
