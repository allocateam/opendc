package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.sc20.telemetry.Event

public class JobLifecycleEvent(
    override val timestamp: Long,
    public val jobID: String,
): Event("jobLifetime") {
    public var submissionTime: Long = 0
    public var startTime: Long = 0
    public var finishTime: Long = 0
}
