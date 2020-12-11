package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.sc20.telemetry.Event

public data class TaskThroughputEvent(
    override val timestamp: Long,
    public val tasksPerSecond: Double
) : Event("taskThroughput")
