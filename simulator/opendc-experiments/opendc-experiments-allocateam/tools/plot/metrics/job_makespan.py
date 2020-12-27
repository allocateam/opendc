from .metric import Metric, metric_path
import pandas as pd
import math


class JobMakespanMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "job_makespan"
        self.x_axis_label = "Job makespan (seconds)"

    def get_data(self, run):
        job_df = pd.read_parquet(metric_path("job-lifecycle", run))
        task_df = pd.read_parquet(metric_path("task-lifecycle", run))

        for _, job in job_df.iterrows():
            tasks = task_df[task_df.job_id == job.job_id]

            # job makespan: time elapsed from first-task submission of job until last completion of task from job
            first_task_submission_time = tasks.submission_time.min()
            last_task_finish_time = tasks.finish_time.max()
            makespan = (last_task_finish_time - first_task_submission_time) // 1000
            if math.isnan(makespan):
                continue

            yield makespan
