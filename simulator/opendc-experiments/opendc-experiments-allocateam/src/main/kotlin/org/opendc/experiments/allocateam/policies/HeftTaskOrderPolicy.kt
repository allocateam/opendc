package org.opendc.experiments.allocateam.policies

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

import mu.KotlinLogging
import org.opendc.workflows.service.JobState
import org.opendc.workflows.service.StageWorkflowSchedulerListener
import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState
import org.opendc.workflows.service.stage.task.TaskEligibilityPolicy
import org.opendc.workflows.service.stage.task.TaskOrderPolicy
import org.opendc.workflows.workload.Task
import java.lang.Exception
import java.util.*
import kotlin.collections.HashMap
import kotlin.random.Random


private val logger = KotlinLogging.logger {}


/**
 * A [TaskEligibilityPolicy] that limits the number of active tasks of a job in the system.
 */
public data class HeftTaskOrderPolicy(public val heftPolicyState: HeftPolicyState) : TaskOrderPolicy {

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

    override fun toString(): String {
        return "Heft task order Policy"
    }
}
