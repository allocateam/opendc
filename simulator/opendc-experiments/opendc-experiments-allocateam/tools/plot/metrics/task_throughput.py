from .metric import Metric, metric_path
import pandas as pd


class TaskThroughputMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "task_throughput"
        self.x_axis_label = "Task throughput (tasks per hour)"

    def get_data(self, run):
        run_duration = pd.read_parquet(metric_path("run-duration", run)).run_duration[0]
        task_df = pd.read_parquet(metric_path("task-lifecycle", run))
        print(run.topology, run.workload_name, len(task_df))
        yield len(task_df) / (run_duration // 1000 // 60 // 60)
