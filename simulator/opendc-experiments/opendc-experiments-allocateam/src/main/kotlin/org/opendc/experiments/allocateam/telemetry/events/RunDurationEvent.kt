package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.sc20.telemetry.Event

public class RunDurationEvent(
    override val timestamp: Long,
    public val duration: Long
): Event("runDuration")
