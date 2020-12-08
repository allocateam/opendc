package org.opendc.experiments.allocateam.experiment

/**
 * The status of a run. 
 *
 * IDLE: it has not been invoked yet.
 * RUNNING: it has been invoked and is currently running.
 * FINISHED: it has been invoked and has finished running.
 */
public enum class RunStatus {
    IDLE,
    RUNNING,
    FINISHED
}
