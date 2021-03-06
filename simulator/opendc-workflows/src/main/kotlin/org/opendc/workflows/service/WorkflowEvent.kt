/*
 * Copyright (c) 2020 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.workflows.service

import org.opendc.workflows.workload.Job

/**
 * An event emitted by the [WorkflowService].
 */
public sealed class WorkflowEvent {
    /**
     * The [WorkflowService] that emitted the event.
     */
    public abstract val service: WorkflowService

    /**
     * This event is emitted when a job has been submitted to the scheduler.
     */
    public data class JobSubmitted(
        override val service: WorkflowService,
        public val jobState: JobState,
        public val time: Long
    ) : WorkflowEvent()

    /**
     * This event is emitted when a job has become active.
     */
    public data class JobStarted(
        override val service: WorkflowService,
        public val jobState: JobState,
        public val time: Long
    ) : WorkflowEvent()

    /**
     * This event is emitted when a job has finished processing.
     */
    public data class JobFinished(
        override val service: WorkflowService,
        public val job: Job,
        public val time: Long
    ) : WorkflowEvent()

    /**
     * This event is emitted when a task of a job has been submitted for execution.
     */
    public data class TaskSubmitted(
        override val service: WorkflowService,
        public val job: Job,
        public val task: TaskState,
        public val time: Long
    ) : WorkflowEvent()

    /**
     * This event is emitted when a task of a job has started processing.
     */
    public data class TaskStarted(
        override val service: WorkflowService,
        public val job: Job,
        public val task: TaskState,
        public val time: Long
    ) : WorkflowEvent()

    /**
     * This event is emitted when a task of a job has started processing.
     */
    public data class TaskFinished(
        override val service: WorkflowService,
        public val job: Job,
        public val task: TaskState,
        public val time: Long
    ) : WorkflowEvent()
}
