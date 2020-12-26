from .metric import Metric, metric_path
import pandas as pd


class JobTurnaroundTimeMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "job_turnaround"
        self.x_axis_label = "Turnaround time (seconds)"

    def get_data(self, run):
        job_df = pd.read_parquet(metric_path("job-lifecycle", run))
        times = (job_df.finish_time - job_df.start_time) // 1000
        for row in times:
            yield row
