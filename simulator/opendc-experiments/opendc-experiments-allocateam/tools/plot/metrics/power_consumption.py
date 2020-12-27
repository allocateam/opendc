from .metric import Metric, metric_path
import pandas as pd
import numpy as np


class PowerConsumptionMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "power_consumption"
        self.x_axis_label = "Power Consumption (watts)"

    def get_data(self, run):
        run_duration = pd.read_parquet(metric_path("run-duration", run)).run_duration[0]
        df = pd.read_parquet(metric_path("power-consumption", run))
        power_consumption = []
        for node in df.server_id.unique():
            timestamps = df[df.server_id == node].sort_values(by='timestamp')
            durations = np.array(list(timestamps.timestamp[1:]) + [run_duration]) - np.array(list(timestamps.timestamp))
            timestamps['durations'] = durations / 60 / 60
            timestamps['watt-hours'] = timestamps['wattage'] * timestamps['durations']
            power_consumption.append(
                timestamps['watt-hours'].sum()
            )

        yield sum(power_consumption)
