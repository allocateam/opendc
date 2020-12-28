package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.IdleTimeEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class IdleTimeWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<IdleTimeEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "idle-time-writer"

    public companion object {
        private val convert: (IdleTimeEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("timestamp", event.timestamp)
            record.put("server_id", event.serverID)
            record.put("duration", event.duration)
        }

        private val schema: Schema = SchemaBuilder
            .record("idle_time")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("timestamp").type().longType().noDefault()
            .name("server_id").type().stringType().noDefault()
            .name("duration").type().doubleType().noDefault()
            .endRecord()
    }

}
