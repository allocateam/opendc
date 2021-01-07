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
from metrics.plot import ReportSetting1Makespan, ReportSetting1WaitingTime
from metrics.plot import ReportSetting2Makespan, ReportSetting2WaitingTime
from metrics.plot import ReportSetting3
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
                 path: Path,
                 scenario_filter=None):
        self.metric_classes = list(plot_classes.keys())
        self.plot_classes = plot_classes
        self.path = path

        self.metrics = self._preprocess(path, scenario_filter)
        self.make_output_path()

    def make_output_path(self, sub_dir=None):
        output_path = Path(self.OUTPUT_PATH)
        path = output_path / sub_dir if sub_dir is not None else output_path
        path.mkdir(parents=True, exist_ok=True)

    def _preprocess(self, path: Path, scenario_filter) -> List[Metric]:
        experiments = pd.read_parquet(path / "experiments.parquet")
        if scenario_filter is not None:
            experiments = experiments[scenario_filter(experiments)]
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

def generate_report_plots(args):
    # Setting 1
    report_plots = {
        metrics.JobMakespanMetric: [ReportSetting1Makespan],
        metrics.JobWaitingTimeMetric: [ReportSetting1WaitingTime],
    }

    scenario_filter = lambda df: df.workload_name == "spec_trace-2"

    plotter = Plotter(report_plots, args.path, scenario_filter)
    plotter.plot_all()

    # Setting 2

    report_plots = {
        metrics.JobMakespanMetric: [ReportSetting2Makespan],
        metrics.JobWaitingTimeMetric: [ReportSetting2WaitingTime]
    }

    scenario_filter = lambda df: df.topology == "medium"

    plotter = Plotter(report_plots, args.path, scenario_filter)
    plotter.plot_all()

    # Setting 3

    groups = [
        ([metrics.JobMakespanMetric, metrics.JobWaitingTimeMetric], "s3-enduser-1"),
        ([metrics.JobTurnaroundTimeMetric, metrics.TaskThroughputMetric], "s3-enduser-2"),
        ([metrics.IdleTimeMetric, metrics.PowerConsumptionMetric], "s3-datacenter")
    ]
    scenario_filter = lambda df: (df.topology == "medium") & (df.workload_name == "spec_trace-2")

    for multi_metrics, filename in groups:
        report_plots = {m: [] for m in multi_metrics}
        plotter = Plotter(report_plots, args.path, scenario_filter)
        dfs = []
        for metric in plotter.metrics:
            df = metric.metric_dataframe()
            df.rename(columns={metric.name: "value"}, inplace=True)
            df['metric'] = metric.name
            dfs.append(df)

        plot = ReportSetting3(filename)
        plot.generate(pd.concat(dfs), None, plotter, None)


def main():
    """Usage: python3 plot.py <path_to_data_dir>"""

    report_plots = True

    parser = argparse.ArgumentParser(description="Plot metrics for the Allocateam experiment.")
    parser.add_argument(
        "path",
        nargs='?',
        type=str,
        help="The path to the input csv file.",
        default=metrics.metric.BASE_DATA_PATH,
    )
    args = parser.parse_args()

    sns.set(
        style="darkgrid",
        font_scale=1.6
    )

    if report_plots:
        generate_report_plots(args)
    else:
        all_plots = {
            metrics.JobTurnaroundTimeMetric: [bar_plot, violin_plot],
            metrics.TaskThroughputMetric: [bar_plot],
            metrics.PowerConsumptionMetric: [bar_plot, violin_plot],
            metrics.IdleTimeMetric: [bar_plot],
            metrics.JobWaitingTimeMetric: [bar_plot, violin_plot],
            metrics.JobMakespanMetric: [bar_plot, violin_plot],
        }
        plotter = Plotter(all_plots, args.path)
        plotter.plot_all()


if __name__ == "__main__":
    main()
