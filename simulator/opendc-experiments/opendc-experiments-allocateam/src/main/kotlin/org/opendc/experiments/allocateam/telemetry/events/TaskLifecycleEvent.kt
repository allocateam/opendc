package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.sc20.telemetry.Event

public class TaskLifecycleEvent(
    override val timestamp: Long,
    public val jobID: String,
    public val taskID: String,
): Event("taskLifetime") {
    public var submissionTime: Long = 0
    public var startTime: Long = 0
    public var finishTime: Long = 0
    public var serverID: String = ""
}
