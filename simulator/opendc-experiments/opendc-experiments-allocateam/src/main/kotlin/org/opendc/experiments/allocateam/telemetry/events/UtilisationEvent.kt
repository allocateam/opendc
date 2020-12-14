package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.sc20.telemetry.Event

public class UtilisationEvent(
    override val timestamp: Long,
    public val serverID: String,
    public val utilisation: Double,
    public val startTime: Long,
    public val endTime: Long
) : Event("utilisation")
