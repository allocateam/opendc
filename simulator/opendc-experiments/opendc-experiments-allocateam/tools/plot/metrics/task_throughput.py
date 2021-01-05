from .metric import Metric, metric_path
import pandas as pd


class TaskThroughputMetric(Metric):
    def __init__(self, plot, scenarios):
        super().__init__(plot, scenarios)
        self.name = "task_throughput"
        self.x_axis_label = "Task throughput (tasks per hour)"

    def get_data(self, scenario):
        run_duration = pd.read_parquet(metric_path("run-duration", scenario)).run_duration[0]
        task_df = pd.read_parquet(metric_path("task-lifecycle", scenario))
        print(scenario.topology, scenario.workload_name, len(task_df))
        yield len(task_df) / (run_duration // 1000 // 60 // 60)
