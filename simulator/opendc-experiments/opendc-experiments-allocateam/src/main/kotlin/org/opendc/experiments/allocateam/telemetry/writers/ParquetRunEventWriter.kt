package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.RunEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

/**
 * A Parquet event writer for [RunEvent]s.
 */
public class ParquetRunEventWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<RunEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "run-writer"

    public companion object {
        private val convert: (RunEvent, GenericData.Record) -> Unit = { event, record ->
            val run = event.run
            val scenario = run.parent
            val portfolio = scenario.parent
            record.put("portfolio_id", portfolio.id)
            record.put("portfolio_name", portfolio.name)
            record.put("scenario_id", scenario.id)
            record.put("run_id", run.id)
            record.put("repetitions", scenario.repetitions)
            record.put("topology", scenario.topology.name)
            record.put("workload_name", scenario.workload.name)
            record.put("allocation_policy", scenario.allocationPolicy)
        }

        private val schema: Schema = SchemaBuilder
            .record("runs")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("portfolio_id").type().intType().noDefault()
            .name("portfolio_name").type().stringType().noDefault()
            .name("scenario_id").type().intType().noDefault()
            .name("run_id").type().intType().noDefault()
            .name("repetitions").type().intType().noDefault()
            .name("topology").type().stringType().noDefault()
            .name("workload_name").type().stringType().noDefault()
            .name("allocation_policy").type().stringType().noDefault()
            .endRecord()
    }
}
