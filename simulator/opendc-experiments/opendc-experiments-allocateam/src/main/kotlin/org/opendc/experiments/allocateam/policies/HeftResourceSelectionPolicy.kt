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
import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy
import org.opendc.workflows.service.stage.task.TaskEligibilityPolicy


private val logger = KotlinLogging.logger {}


/**
 * A [TaskEligibilityPolicy] that limits the number of active tasks of a job in the system.
 */
public class HeftResourceSelectionPolicy(public var heftPolicyState: HeftPolicyState) : ResourceSelectionPolicy {

    private val logger = KotlinLogging.logger {}

    private var jobsStarted = 0
    private var jobsFinished = 0

    override fun invoke(scheduler: StageWorkflowService): (List<Node>, TaskState) -> Node? =
        object : (List<Node>, TaskState) -> Node?, StageWorkflowSchedulerListener {

            init {
                scheduler.addListener(this)
                heftPolicyState.addNodes(scheduler.nodes)
            }

            override fun jobStarted(job: JobState) {
//                logger.info ("Job ${job.job.uid} started: Total: ${++jobsStarted}")
                heftPolicyState.addJob(job)
            }

            override fun jobFinished(job: JobState) {
//                logger.info ("Job ${job.job.uid} finished: Total: ${++jobsFinished}")
                heftPolicyState.removeJob(job)
            }

            override fun invoke(availableNodes: List<Node>, taskState: TaskState): Node? {
                val agent = heftPolicyState.getAgentForTaskState(taskState) ?: return null

                return if(availableNodes.contains(agent)) {
                    agent
                } else {
                    null
                }
            }

            override fun toString(): String {
                return "HeftPolicy() - Resource"
            }
        }
}
