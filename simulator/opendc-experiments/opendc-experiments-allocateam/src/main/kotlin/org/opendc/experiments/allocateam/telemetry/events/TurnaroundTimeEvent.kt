package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.sc20.telemetry.Event

public data class TurnaroundTimeEvent(
    override val timestamp: Long,
    val duration: Long
) : Event("turnaroundTime")
