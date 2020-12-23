package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.UtilisationEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class UtilisationWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<UtilisationEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "utilisation-writer"

    public companion object {
        private val convert: (UtilisationEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("timestamp", event.timestamp)
            record.put("server_id", event.serverID)
            record.put("utilisation", event.utilisation)
            record.put("start_time", event.startTime)
            record.put("end_time", event.endTime)

        }

        private val schema: Schema = SchemaBuilder
            .record("idle_time")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("timestamp").type().longType().noDefault()
            .name("server_id").type().stringType().noDefault()
            .name("utilisation").type().doubleType().noDefault()
            .name("start_time").type().longType().noDefault()
            .name("end_time").type().longType().noDefault()
            .endRecord()
    }

}
