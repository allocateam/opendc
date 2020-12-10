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

package org.opendc.experiments.allocateam.policies

import mu.KotlinLogging
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.task.TaskEligibilityPolicy
import org.opendc.workflows.workload.Task
import java.util.*

private val logger = KotlinLogging.logger {}


/**
 * A [TaskEligibilityPolicy] that limits the number of active tasks of a job in the system.
 */
public data class RoundRobinPolicy(public val quanta: Int) : TaskEligibilityPolicy {

    public data class QueuedJob(val jobId: UUID, var tasks: MutableSet<Task>, var totalNumberOfTasks: Int)

    override fun invoke(scheduler: StageWorkflowService): TaskEligibilityPolicy.Logic =
        object : TaskEligibilityPolicy.Logic, StageWorkflowSchedulerListener {
            private var numScheduledSoFar = 0
            private val activeQueue: Queue<QueuedJob> = LinkedList()
            private var numJobsStarted = 0
            private var numJobsFinished = 0

            init {
                scheduler.addListener(this)
            }

            override fun jobStarted(job: JobState) {
                logger.info("Job started: ${job.job.uid} number of jobs started: ${++numJobsStarted}")
                val queuedJob = QueuedJob(job.job.uid, mutableSetOf(), 0)
                activeQueue.add(queuedJob)
            }

            override fun jobFinished(job: JobState) {
                logger.info("Job finished: ${job.job.uid} number of jobs finished: ${++numJobsFinished}")
            }

            override fun taskReady(task: TaskState) {
                val queuedJobs = activeQueue.filter { it.jobId.equals(task.job.job.uid) }
                if(queuedJobs.isNotEmpty()) {
                    queuedJobs[0].tasks.add(task.task)
                    queuedJobs[0].totalNumberOfTasks += 1
                    return
                }
                val queuedJob = QueuedJob(task.job.job.uid, mutableSetOf(task.task), 1)
                activeQueue.add(queuedJob)
            }

            override fun invoke(task: TaskState): TaskEligibilityPolicy.Advice {
                // If we already scheduled a quanta amount during this batch, we stop scheduling any more tasks for this
                // batch
                if(numScheduledSoFar + 1 > quanta) {
                    logger.info("Quanta of $quanta exceeded")
                    numScheduledSoFar = 0
                    return TaskEligibilityPolicy.Advice.STOP
                }

                val job: QueuedJob = activeQueue.peek()

                // We check to see if the current task is part of the same job as the top of the job queue
                if(!task.job.job.uid.equals(job.jobId)) {
                    return TaskEligibilityPolicy.Advice.DENY
                }

                var taskIsQueued = false

                // We check to see if the task is in the queued job set
                for(queuedTask in job.tasks) {
                    if(queuedTask.equals(task.task)) {
                        taskIsQueued = true
                        break
                    }
                }

                if(!taskIsQueued) {
                    return TaskEligibilityPolicy.Advice.DENY
                }

                // We remove the task from the list of tasks of the queued job to execute
                job.tasks.remove(task.task)

                // If we finished all the tasks from the queued job set, we pop it from the queue
                if(job.tasks.size == 0) {
                    activeQueue.remove()

                // Otherwise we create a new queued job with remaining of the tasks to be executed
                } else if(job.totalNumberOfTasks - job.tasks.size == quanta) {
                    var remainingJob = activeQueue.remove()
                    remainingJob.totalNumberOfTasks = remainingJob.tasks.size
                    activeQueue.add(remainingJob)
                }

                numScheduledSoFar++
                logger.info("Scheduling job: ${task.job.job.uid}, task: ${task.task.uid}")
                return TaskEligibilityPolicy.Advice.ADMIT
            }
        }

    override fun toString(): String = "Round-Robin-Quanta-($quanta)"
}
