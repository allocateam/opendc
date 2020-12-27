package org.opendc.experiments.allocateam.telemetry.writers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.opendc.experiments.allocateam.telemetry.events.JobLifecycleEvent
import org.opendc.experiments.sc20.telemetry.parquet.ParquetEventWriter
import java.io.File

public class JobLifecycleWriter(path: File, bufferSize: Int) :
    ParquetEventWriter<JobLifecycleEvent>(path, schema, convert, bufferSize) {

    override fun toString(): String = "job-lifecycle-writer"

    public companion object{
        private val convert: (JobLifecycleEvent, GenericData.Record) -> Unit = { event, record ->
            record.put("job_id", event.jobID)
            record.put("submission_time", event.submissionTime)
            record.put("start_time", event.startTime)
            record.put("finish_time", event.finishTime)
        }

        private val schema:Schema=SchemaBuilder
            .record("lifecycle")
            .namespace("org.opendc.experiments.allocateam")
            .fields()
            .name("job_id").type().stringType().noDefault()
            .name("submission_time").type().longType().noDefault()
            .name("start_time").type().longType().noDefault()
            .name("finish_time").type().longType().noDefault()
            .endRecord()
    }
}
