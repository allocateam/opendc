package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.TaskThroughputEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class TaskThroughputWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<TaskThroughputEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "task-throughput"

    public companion object {
        private val convert: (TaskThroughputEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("timestamp", event.timestamp)
            record.put("tasks_per_second", event.tasksPerSecond)
        }

        private val schema: Schema = SchemaBuilder
            .record("task_throughput")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("timestamp").type().longType().noDefault()
            .name("tasks_per_second").type().doubleType().noDefault()
            .endRecord()
    }
}
