package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.TurnaroundTimeEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class TurnaroundTimeWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<TurnaroundTimeEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "task-throughput"

    public companion object {
        private val convert: (TurnaroundTimeEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("timestamp", event.timestamp)
            record.put("duration", event.duration)
        }

        private val schema: Schema = SchemaBuilder
            .record("turnaround_time")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("timestamp").type().longType().noDefault()
            .name("duration").type().longType().noDefault()
            .endRecord()
    }
}
