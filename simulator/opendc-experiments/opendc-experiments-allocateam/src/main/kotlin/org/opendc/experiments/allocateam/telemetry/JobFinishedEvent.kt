package org.opendc.experiments.allocateam.telemetry

import org.opendc.experiments.sc20.telemetry.Event

public data class JobFinishedEvent(
    override val timestamp: Long,
    val duration: Long
) : Event("jobFinished")
