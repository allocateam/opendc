package org.opendc.experiments.allocateam.experiment.monitor

import mu.KotlinLogging
import org.opendc.compute.core.metal.Node
import org.opendc.compute.simulator.SimWorkloadImage
import org.opendc.experiments.allocateam.telemetry.events.*
import org.opendc.experiments.allocateam.telemetry.writers.*
import org.opendc.simulator.compute.workload.SimFlopsWorkload
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.WorkflowEvent
import org.opendc.workflows.workload.Job
import java.io.File

private val logger = KotlinLogging.logger {}

public class ParquetExperimentMonitor(base: File, partition: String, bufferSize: Int) {
    private var startTime: Long = 0
    private var finishedTasks = 0

    private val lastFinishTimePerNode = mutableMapOf<Node, Long>()
    private val idleTimePerNode = mutableMapOf<Node, Long>()
    private val submissionTimesPerJob = mutableMapOf<Job, Long>()

    private val turnaroundTimeWriter = TurnaroundTimeWriter(
        File(base, "turnaround-time/$partition/data.parquet"),
        bufferSize
    )

    private val taskThroughputWriter = TaskThroughputWriter(
        File(base, "task-throughput/$partition/data.parquet"),
        bufferSize
    )

    private val powerConsumptionWriter = PowerConsumptionWriter(
        File(base, "power-consumption/$partition/data.parquet"),
        bufferSize
    )

    private val idleTimeWriter = IdleTimeWriter(
        File(base, "idle-time/$partition/data.parquet"),
        bufferSize
    )

    private val utilisationWriter = UtilisationWriter(
        File(base, "utilisation/$partition/data.parquet"),
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

    public fun reportTaskStarted(time: Long, task: TaskState) {
        lastFinishTimePerNode[task.host]?.let { lastFinishTime ->
            task.host?.let {
                idleTimePerNode[it] = idleTimePerNode.getOrDefault(it, 0) + time - lastFinishTime
            }
            lastFinishTimePerNode -= task.host!!
        }
    }

    public fun reportTaskFinished(time: Long, task: TaskState) {
        this.finishedTasks += 1
        task.host?.let {
            lastFinishTimePerNode[it] = time

            utilisationWriter.write(
                UtilisationEvent(
                    time,
                    it.name,
                    ((task.task.image as SimWorkloadImage).workload as SimFlopsWorkload).utilization,
                    task.startedAt,
                    time
                )
            )
        }
    }

    public fun reportJobStarted(event: WorkflowEvent.JobStarted) {
        // TODO(gm): create a proper WorkflowEvent for this
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
        for ((node, duration) in idleTimePerNode) {
            idleTimeWriter.write(
                IdleTimeEvent(
                    0,
                    node.name,
                    duration
                )
            )
        }

        turnaroundTimeWriter.close()
        taskThroughputWriter.close()
        powerConsumptionWriter.close()
        idleTimeWriter.close()
        utilisationWriter.close()
    }
}
