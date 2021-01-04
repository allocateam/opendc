from .metric import Metric, metric_path
import pandas as pd


class TaskThroughputMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "task_throughput"
        self.x_axis_label = "Task throughput (tasks per hour)"

    def get_data(self, run):
        task_df = pd.read_parquet(metric_path("task-lifecycle", run))
        run_duration = task_df.finish_time.max()
        yield len(task_df) / ((run_duration // 1000) / 60 / 60)
