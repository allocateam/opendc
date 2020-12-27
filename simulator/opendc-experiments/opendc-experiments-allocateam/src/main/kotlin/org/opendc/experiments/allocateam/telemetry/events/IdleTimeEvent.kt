package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.sc20.telemetry.Event

public class IdleTimeEvent(
    override val timestamp: Long,
    public val serverID: String,
    public val duration: Long
) : Event("idleTime")
