from .metric import Metric, metric_path
import pandas as pd


class IdleTimeMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "idle_time"
        self.x_axis_label = "Idle time (in seconds)"

    def get_data(self, run):
        run_duration = pd.read_parquet(metric_path("run-duration", run)).run_duration[0]
        df = pd.read_parquet(metric_path("task-lifecycle", run))
        df['duration'] = df.finish_time - df.start_time
        yield ((run_duration - df.duration.sum()) / (run_duration // 1000)) * 100
