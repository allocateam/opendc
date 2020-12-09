package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.RunCompletedEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

/**
 * A Parquet event writer for [RunCompletedEvent]s.
 */
public class ParquetRunCompletedEventWriter(path: File) :
    ParquetEventWriter<RunCompletedEvent>(path, schema, convert) {

    override fun toString(): String = "run-completed-event-writer"

    public companion object {
        private val convert: (RunCompletedEvent, GenericData.Record) -> Unit = { event, record ->
            val run = event.run
            val scenario = run.parent
            val portfolio = scenario.parent
            val runMetrics = event.runMetrics
            record.put("portfolio_id", portfolio.id)
            record.put("portfolio_name", portfolio.name)
            record.put("scenario_id", scenario.id)
            record.put("run_id", run.id)
            record.put("repetitions", scenario.repetitions)
            record.put("topology", scenario.topology.name)
            record.put("workload_name", scenario.workload.name)
            record.put("resource_selection_policy", scenario.resourceSelectionPolicy)
            record.put("task_eligibility_policy", scenario.taskEligibilityPolicy)
            record.put("task_throughput", runMetrics.taskThroughput)
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
            .name("resource_selection_policy").type().stringType().noDefault()
            .name("task_eligibility_policy").type().stringType().noDefault()
            .name("task_throughput").type().doubleType().noDefault()
            .endRecord()
    }
}
