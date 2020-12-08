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

package org.opendc.workflows.service.stage.task

import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.workload.Job
import org.opendc.workflows.workload.Task
import java.util.*


/**
 * A [TaskEligibilityPolicy] that limits the number of active tasks of a job in the system.
 */
public data class RoundRobinPolicy(public val quanta: Int) : TaskEligibilityPolicy {

    public data class QueuedJob(val jobId: UUID, var tasks: MutableSet<Task>, var totalNumberOfTasks: Int)

    override fun invoke(scheduler: StageWorkflowService): TaskEligibilityPolicy.Logic =
        object : TaskEligibilityPolicy.Logic, StageWorkflowSchedulerListener {
            private var numScheduledSoFar = 0
            private val activeQueue: Queue<QueuedJob> = LinkedList()

            init {
                scheduler.addListener(this)
            }

            override fun jobStarted(job: JobState) {
                val queuedJob = QueuedJob(job.job.uid, mutableSetOf(), 0)
                activeQueue.add(queuedJob)
//                println("New job: jobId: ${queuedJob.jobId}, Num tasks: ${queuedJob.totalNumberOfTasks}")
            }

            override fun jobFinished(job: JobState) {
            }

            override fun taskReady(task: TaskState) {
//                println("Task ready ${task.task.uid}, job id: ${task.job.job.uid}")
                val queuedJobs = activeQueue.filter { it.jobId.equals(task.job.job.uid) }
                if(queuedJobs.isNotEmpty()) {
                    queuedJobs[0].tasks.add(task.task)
                    queuedJobs[0].totalNumberOfTasks += 1
                    return
                }

//                println("null task ready for jobid: ${task.job.job.uid}")
                val queuedJob = QueuedJob(task.job.job.uid, mutableSetOf(task.task), 1)
                activeQueue.add(queuedJob)
            }

            override fun taskFinished(task: TaskState) {
//                active.merge(task.job, -1, Int::plus)
            }

            override fun invoke(task: TaskState): TaskEligibilityPolicy.Advice {
                // If we already scheduled a quanta amount during this batch, we stop scheduling any more tasks for this
                // batch
                if(numScheduledSoFar + 1 > quanta) {
//                    println("Exceeded quota, stopping!")
                    numScheduledSoFar = 0
                    return TaskEligibilityPolicy.Advice.STOP
                }

                val job:QueuedJob = activeQueue.peek()
//                println("Job in queue: ${job.jobId} num_tasks_left for job: ${job.tasks.size}, queue size: ${activeQueue.size}")

                // We check to see if the current task is part of the same job as the top of the job queue
                if(!task.job.job.uid.equals(job.jobId)) {
//                    println("Job task does not have same id as task for scheduling")
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
//                    println("Task is not in the job ${job.jobId} list of tasks")
                    return TaskEligibilityPolicy.Advice.DENY
                }

                // We remove the task from the list of tasks of the queued job to execute
//                println("Number of tasks for job ${job.jobId}: ${job.tasks.size}")
                job.tasks.remove(task.task)
//                println("Number of tasks for job ${job.jobId}: ${job.tasks.size}")

                // If we finished all the tasks from the queued job set, we pop it from the queue
                if(job.tasks.size == 0) {
                    activeQueue.remove()

                // Otherwise we create a new queued job with remaining of the tasks to be executed
                } else if(job.totalNumberOfTasks - job.tasks.size == quanta) {
                    var remainingJob = activeQueue.remove()
                    remainingJob.totalNumberOfTasks = remainingJob.tasks.size
                    activeQueue.add(remainingJob)
                }

//                println("Scheduled task: ${task.task.uid} of job: ${job.jobId}")
                numScheduledSoFar++
                return TaskEligibilityPolicy.Advice.ADMIT
            }
        }

    override fun toString(): String = "Limit-Active-Job($quanta)"
}
