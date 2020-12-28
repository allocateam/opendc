package org.opendc.experiments.allocateam.policies.heft

import mu.KotlinLogging
import org.opendc.compute.core.metal.Node
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.resource.ResourceSelectionPolicy
import org.opendc.workflows.service.stage.task.TaskEligibilityPolicy


private val logger = KotlinLogging.logger {}


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

            override fun toString(): String = "HeftPolicy() - Resource"
        }
}
