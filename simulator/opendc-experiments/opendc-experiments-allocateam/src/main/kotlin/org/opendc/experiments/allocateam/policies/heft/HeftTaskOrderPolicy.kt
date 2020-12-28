package org.opendc.experiments.allocateam.policies.heft

import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.task.TaskOrderPolicy
import java.util.*


public data class HeftTaskOrderPolicy(public val heftPolicyState: HeftPolicyState): TaskOrderPolicy {

    override fun invoke(scheduler: StageWorkflowService): Comparator<TaskState> =
        object : Comparator<TaskState>, StageWorkflowSchedulerListener {

            override fun compare(o1: TaskState, o2: TaskState): Int {
                try {
                    val orders = heftPolicyState.heftOrdersJobs
                    val jobsOn = heftPolicyState.heftJobsOn

                    val jobO1 = o1.job
                    val jobO2 = o2.job

                    val agentO1 = jobsOn[jobO1]?.get(o1)
                    val agentO2 = jobsOn[jobO2]?.get(o2)

                    val eventO1 = orders[jobO1]?.get(agentO1)?.filter { event -> event.task == o1 }?.get(0)
                    val eventO2 = orders[jobO2]?.get(agentO2)?.filter { event -> event.task == o2 }?.get(0)

                    return if (agentO1 != agentO2) {
                        val value = compareValuesBy(eventO1, eventO2, { it?.end })
                        value
                    } else {
                        if (eventO1!!.end <= eventO2!!.start!!) {
                            -1
                        } else {
                            1
                        }
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                    throw e
                }
            }
        }

    override fun toString(): String = "Heft task order Policy"
}
