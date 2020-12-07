package org.opendc.experiments.allocateam.telemetry.events

import org.opendc.experiments.allocateam.experiment.Run
import org.opendc.experiments.allocateam.telemetry.RunMetrics
import org.opendc.experiments.sc20.telemetry.Event

/**
 * A completed run, along with its metrics that were accumulated.
 */
public data class RunCompletedEvent(
    public val run: Run,
    public val runMetrics: RunMetrics,
    override val timestamp: Long
) : Event("runCompleted")
