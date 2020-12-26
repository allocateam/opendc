package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.TaskLifecycleEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class TaskLifecycleWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<TaskLifecycleEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "task-lifecycle-writer"

    public companion object {
        private val convert: (TaskLifecycleEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("task_id", event.taskID)
            record.put("job_id", event.jobID)
            record.put("server_id", event.serverID)
            record.put("submission_time", event.submissionTime)
            record.put("start_time", event.startTime)
            record.put("finish_time", event.finishTime)
        }

        private val schema: Schema = SchemaBuilder
            .record("task_lifetime")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("task_id").type().stringType().noDefault()
            .name("job_id").type().stringType().noDefault()
            .name("server_id").type().stringType().noDefault()
            .name("submission_time").type().longType().noDefault()
            .name("start_time").type().longType().noDefault()
            .name("finish_time").type().longType().noDefault()
            .endRecord()
    }
}
