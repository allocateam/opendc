package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.PowerConsumptionEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class PowerConsumptionWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<PowerConsumptionEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "power-consumption-event-writer"

    public companion object {
        private val convert: (PowerConsumptionEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("timestamp", event.timestamp)
            record.put("server_id", event.serverID)
            record.put("wattage", event.wattage)
        }

        private val schema: Schema = SchemaBuilder
            .record("power_consumption")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("timestamp").type().longType().noDefault()
            .name("server_id").type().stringType().noDefault()
            .name("wattage").type().doubleType().noDefault()
            .endRecord()
    }

}
