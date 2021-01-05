from .metric import Metric, metric_path
import pandas as pd


class IdleTimeMetric(Metric):
    def __init__(self, plot, scenarios):
        super().__init__(plot, scenarios)
        self.name = "idle_time"
        self.x_axis_label = "Average idle percentage (per machine)"

    def get_data(self, scenario):
        job_df = pd.read_parquet(metric_path("job-lifecycle", scenario))
        task_df = pd.read_parquet(metric_path("task-lifecycle", scenario))
        task_df["duration"] = task_df.finish_time - task_df.start_time

        server_stats = task_df.groupby("server_id").aggregate({"duration": ["sum"]})
        workload_duration = job_df.finish_time.max()
        server_stats["idle_time"] = workload_duration - server_stats.duration

        sizes = {
            "small": 32,
            "medium": 256,
            "large": 10000
        }
        topology_size = sizes[scenario.topology]
        unused_servers = topology_size - len(server_stats)
        unused_server_time = workload_duration * unused_servers
        if unused_servers > 0:
            print(f"Warning: topology not fully utilised ({unused_servers} machine(s) that are 100% idle)")
            print("topology: {}, allocation policy: {}, workload: {}".format(
                scenario.topology,
                scenario.allocation_policy,
                scenario.workload_name
            ))
            print()
        mean_idle_time_per_server = (server_stats.idle_time.sum() + unused_server_time) / topology_size
        yield mean_idle_time_per_server / workload_duration * 100
