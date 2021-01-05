from .metric import Metric, metric_path
import pandas as pd


class IdleTimeMetric(Metric):
    def __init__(self, plot, scenarios):
        super().__init__(plot, scenarios)
        self.name = "idle_time"
        self.x_axis_label = "Idle time (in seconds)"

    def get_data(self, scenario):
        run_duration = pd.read_parquet(metric_path("run-duration", scenario)).run_duration[0]
        df = pd.read_parquet(metric_path("task-lifecycle", scenario))
        df['duration'] = df.finish_time - df.start_time
        yield ((run_duration - df.duration.sum()) / (run_duration // 1000)) * 100
