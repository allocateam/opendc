import org.opendc.workflows.service.StageWorkflowService
import org.opendc.workflows.service.TaskState

public data class RoundRobinPolicy(public val ascending: Boolean = true) : TaskOrderPolicy {
    override fun invoke(scheduler: StageWorkflowService): Comparator<TaskState> = compareBy<TaskState> {
        /* TODO: Implement the round robin pollicy */
    }

    override fun toString(): String {
        return "Submission-Time(${if (ascending) "asc" else "desc"})"
    }
}
