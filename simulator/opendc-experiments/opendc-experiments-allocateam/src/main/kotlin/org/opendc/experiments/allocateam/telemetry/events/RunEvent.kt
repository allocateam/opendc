package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.allocateam.experiment.Run
import org.opendc.experiments.sc20.telemetry.Event

/**
 * A periodic report of the host machine metrics.
 */
public data class RunEvent(
    public val run: Run,
    override val timestamp: Long
) : Event("run")
