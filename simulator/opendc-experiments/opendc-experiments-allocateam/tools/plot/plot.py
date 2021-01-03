#!/usr/bin/env python3

import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Type

import pandas as pd
import seaborn as sns

from metrics import Metric, Plot
from metrics.plot import MetricWorkloadBarPlot as bar_plot
from metrics.plot import MetricWorkloadViolinPlot as violin_plot
import metrics

def iter_runs(experiments):
    for portfolio_id in experiments['portfolio_id'].unique():
        for scenario_id in experiments['scenario_id'].unique():
            p_id = experiments['portfolio_id'] == portfolio_id
            s_id = experiments['scenario_id'] == scenario_id
            for _, run in experiments[p_id & s_id].iterrows():
                yield run


class Plotter:
    OUTPUT_PATH = f"{Path(__file__).parent.resolve()}/results/{datetime.now():%Y-%m-%d-%H-%M-%S}"

    def __init__(self,
                 plot_classes: Dict[Type[Metric], List[Type[Plot]]],
                 path: Path):
        self.metric_classes = list(plot_classes.keys())
        self.plot_classes = plot_classes
        self.path = path

        self.metrics = self._preprocess(path)
        self.make_output_path()

    def make_output_path(self, sub_dir=None):
        output_path = Path(self.OUTPUT_PATH)
        path = output_path / sub_dir if sub_dir is not None else output_path
        path.mkdir(parents=True, exist_ok=True)

    def _preprocess(self, path: Path) -> List[Metric]:
        experiments = pd.read_parquet(path / "experiments.parquet")
        return [
            metric(self.plot_classes[metric], iter_runs(experiments))
            for metric in self.metric_classes
        ]

    def plot_all(self):
        print("Plotting..")
        for metric in self.metrics:
            metric.generate_plot(self)
            print(f"✅ {metric.name}")

        print(f"Plots successfully stored in {self.OUTPUT_PATH}")


def main():
    """Usage: python3 plot.py <path_to_data_dir>"""

    parser = argparse.ArgumentParser(description="Plot metrics for the Allocateam experiment.")
    parser.add_argument(
        "path",
        nargs='?',
        type=str,
        help="The path to the input csv file.",
        default=metrics.metric.BASE_DATA_PATH,
    )
    args = parser.parse_args()

    all_plots = {
        metrics.JobTurnaroundTimeMetric: [bar_plot, violin_plot],
        metrics.TaskThroughputMetric: [bar_plot, violin_plot],
        metrics.PowerConsumptionMetric: [bar_plot, violin_plot],
        metrics.IdleTimeMetric: [bar_plot],
        metrics.JobWaitingTimeMetric: [bar_plot, violin_plot],
        metrics.JobMakespanMetric: [bar_plot, violin_plot],
    }

    sns.set(
        style="darkgrid",
        font_scale=1.6
    )
    plotter = Plotter(all_plots, args.path)
    plotter.plot_all()


if __name__ == "__main__":
    main()
