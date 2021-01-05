from .metric import Metric, metric_path
import pandas as pd


class TaskThroughputMetric(Metric):
    def __init__(self, plot, scenarios):
        super().__init__(plot, scenarios)
        self.name = "task_throughput"
        self.x_axis_label = "Task throughput (tasks per hour)"

    def get_data(self, scenario):
        task_df = pd.read_parquet(metric_path("task-lifecycle", scenario))
        run_duration = task_df.finish_time.max()
        yield len(task_df) / ((run_duration // 1000) / 60 / 60)
