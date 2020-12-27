package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.RunDurationEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class RunDurationWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<RunDurationEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "run-duration"

    public companion object {
        private val convert: (RunDurationEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("run_duration", event.duration)
        }

        private val schema: Schema = SchemaBuilder
            .record("run_duration")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("run_duration").type().longType().noDefault()
            .endRecord()
    }
}
