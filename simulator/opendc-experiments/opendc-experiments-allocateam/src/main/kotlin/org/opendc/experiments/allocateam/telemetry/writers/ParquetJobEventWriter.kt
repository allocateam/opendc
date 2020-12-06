package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.JobFinishedEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class ParquetJobEventWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<JobFinishedEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "provisioner-writer"

    public companion object {
        private val convert: (JobFinishedEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("timestamp", event.timestamp)
            record.put("duration", event.duration)
        }

        private val schema: Schema = SchemaBuilder
            .record("provisioner_metrics")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("timestamp").type().longType().noDefault()
            .name("duration").type().longType().noDefault()
            .endRecord()
    }
}
