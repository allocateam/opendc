import pandas as pd
from .metric import Metric, metric_path
import math


class JobWaitingTimeMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "job_waiting_time"
        self.x_axis_label = "Job waiting time (in seconds)"

    def get_data(self, run):
        job_df = pd.read_parquet(metric_path("job-lifecycle", run))
        task_df = pd.read_parquet(metric_path("task-lifecycle", run))

        for _, job in job_df.iterrows():
            tasks = task_df[task_df.job_id == job.job_id]

            # job waiting time: time elapsed from the first task-submission of a job
            # to the first start of a task of that job
            job_sub_time = job.submission_time
            first_task_start = tasks.start_time.min()
            waiting_time = (first_task_start - job_sub_time) // 1000
            if math.isnan(waiting_time):
                continue

            yield waiting_time
