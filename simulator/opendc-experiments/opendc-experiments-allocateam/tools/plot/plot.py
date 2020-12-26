#!/usr/bin/env python3

from abc import ABC, abstractmethod

import math
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Type

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

BASE_DATA_PATH = (Path(__file__).parent / "../../data").resolve()


class Plot(ABC):
    @abstractmethod
    def generate(self, data: pd.DataFrame, metric, plotter):
        pass


class Metric(ABC):
    def __init__(self, plot: Type[Plot], runs):
        self.name = "metric"
        self.plot = plot
        self.runs = runs

    def metric_dataframe(self) -> pd.DataFrame:
        result = []
        for run in self.runs:
            for value in self.get_data(run):
                result.append({
                    "topology": run.topology,
                    "workload": run.workload_name,
                    "allocation_policy": run.allocation_policy,
                    self.name: value,
                })
        return pd.DataFrame.from_dict(result)

    def generate_plot(self, plotter):
        self.plot().generate(self.metric_dataframe(), self, plotter)

    @abstractmethod
    def get_data(self, run):
        pass


class JobWaitingTimeMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "job_waiting_time"

    def get_data(self, run):
        job_df = pd.read_parquet(metric_path("job-lifecycle", run))
        task_df = pd.read_parquet(metric_path("task-lifecycle", run))

        for _, job in job_df.iterrows():
            tasks = task_df[task_df.job_id == job.job_id]

            # job waiting time: time elapsed from the first task-submission of a job
            # to the first start of a task of that job
            job_sub_time = job.submission_time
            first_task_start = tasks.start_time.min()
            waiting_time = (first_task_start - job_sub_time) // 1000
            if math.isnan(waiting_time):
                continue

            yield waiting_time


class MetricWorkloadBarPlot(Plot):
    def generate(self, data, metric, plotter):
        plotter._make_output_path(f'{plotter.OUTPUT_PATH}/{metric.name}')
        
        for workload in data.workload.unique():
            plt.figure(figsize=(10, 5))
            sns.barplot(
                data=data[data.workload == workload],
                x=metric.name,
                y="workload",
                hue="allocation_policy",
            )
            plt.tight_layout()
            plt.savefig(f'{plotter.OUTPUT_PATH}/{metric.name}/{workload}.png')


class JobMakespanMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "job_makespan"

    def get_data(self, run):
        job_df = pd.read_parquet(metric_path("job-lifecycle", run))
        task_df = pd.read_parquet(metric_path("task-lifecycle", run))

        for _, job in job_df.iterrows():
            tasks = task_df[task_df.job_id == job.job_id]

            # job makespan: time elapsed from first-task submission of job until last completion of task from job
            first_task_submission_time = tasks.submission_time.min()
            last_task_finish_time = tasks.finish_time.max()
            makespan = (last_task_finish_time - first_task_submission_time) // 1000
            if math.isnan(makespan):
                continue

            yield makespan


class JobTurnaroundTimeMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "job_turnaround"

    def get_data(self, run):
        job_df = pd.read_parquet(metric_path("job-lifecycle", run))
        times = (job_df.finish_time - job_df.start_time) // 1000
        for row in times:
            yield row


class TaskThroughputMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "task_throughput"

    def get_data(self, run):
        run_duration = pd.read_parquet(metric_path("run-duration", run)).run_duration[0]
        task_df = pd.read_parquet(metric_path("task-lifecycle", run))
        yield len(task_df) / (run_duration // 1000)


class PowerConsumptionMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "power_consumption"

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


class IdleTimeMetric(Metric):
    def __init__(self, plot, runs):
        super().__init__(plot, runs)
        self.name = "idle_time"

    def get_data(self, run):
        run_duration = pd.read_parquet(metric_path("run-duration", run)).run_duration[0]
        df = pd.read_parquet(metric_path("task-lifecycle", run))
        df['duration'] = df.finish_time - df.start_time
        yield (run_duration - df.duration.sum()) / run_duration * 100


def metric_path(name, run):
    partition = "portfolio_id={}/scenario_id={}/run_id={}".format(
        run.portfolio_id, run.scenario_id, run.run_id
    )
    return Path(BASE_DATA_PATH) / name / partition / "data.parquet"


def reformat_large_tick_values(tick_val):
    """
    Turns large tick values (in the billions, millions and thousands) such as 4500 into 4.5K and also appropriately turns 4000 into 4K (no zero after the decimal).
    """
    if tick_val >= 1000000000:
        val = round(tick_val / 1000000000, 1)
        new_tick_format = '{:}B'.format(val)
    elif tick_val >= 1000000:
        val = round(tick_val / 1000000, 1)
        new_tick_format = '{:}M'.format(val)
    elif tick_val >= 1000:
        val = round(tick_val / 1000, 1)
        new_tick_format = '{:}K'.format(val)
    elif tick_val < 1000:
        new_tick_format = round(tick_val, 1)
    else:
        new_tick_format = tick_val

    # make new_tick_format into a string value
    new_tick_format = str(new_tick_format)

    # code below will keep 4.5M as is but change values such as 4.0M to 4M since that zero after the decimal isn't needed
    index_of_decimal = new_tick_format.find(".")

    if index_of_decimal != -1 and tick_val > 1:
        value_after_decimal = new_tick_format[index_of_decimal + 1]
        if value_after_decimal == "0":
            # remove the 0 after the decimal point since it's not needed
            new_tick_format = new_tick_format[0:index_of_decimal] + new_tick_format[index_of_decimal + 2:]

    return new_tick_format


def iter_runs(experiments):
    for portfolio_id in experiments['portfolio_id'].unique():
        for scenario_id in experiments['scenario_id'].unique():
            p_id = experiments['portfolio_id'] == portfolio_id
            s_id = experiments['scenario_id'] == scenario_id
            for _, run in experiments[p_id & s_id].iterrows():
                yield run


class Plotter:
    OUTPUT_PATH = f"{Path(__file__).parent.resolve()}/results/{datetime.now():%Y-%m-%d-%H-%m-%d}"

    def __init__(self, metric_classes: List[Type[Metric]], plot_classes: Dict[Type[Metric], Type[Plot]], path: Path):
        self.metric_classes = metric_classes
        self.plot_classes = plot_classes
        self.path = path

        experiments = pd.read_parquet(path / "experiments.parquet")
        self.metrics = self._preprocess(experiments)
        self._make_output_path()

    def _make_output_path(self, sub_dir=None):
        output_path = Path(self.OUTPUT_PATH)
        path = output_path / sub_dir if sub_dir is not None else output_path
        path.mkdir(parents=True, exist_ok=True)

    def _preprocess(self, experiments: pd.DataFrame) -> List[Metric]:
        return [
            metric(self.plot_classes[metric], iter_runs(experiments))
            for metric in self.metric_classes
        ]

    def plot_all(self):
        print("Plotting..")
        for metric in self.metrics:
            metric.generate_plot(self)

        print(f"Plots successfully stored in {self.OUTPUT_PATH}")

    def _plot_metric(self, metric):
        plt.figure(figsize=(10, 5))
        g = sns.barplot(
            data=pd.DataFrame.from_dict(self.data[metric]),
            x=metric,
            y="workload",
            hue="allocation_policy",
            ci=None
        )

        xlabels = [reformat_large_tick_values(x) for x in g.get_xticks()]
        g.set_xticklabels(xlabels)

        g.set_xlabel(self.labels[metric])
        g.set_ylabel("Workload")
        plt.legend(title="Allocation policy", bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
        plt.tight_layout()
        plt.savefig(f'{self.OUTPUT_PATH}/{metric}.png')


def main():
    """Usage: python3 plot.py <path_to_csv>"""

    parser = argparse.ArgumentParser(description="Plot metrics for the Allocateam experiment.")
    parser.add_argument(
        "path",
        nargs='?',
        type=str,
        help="The path to the input csv file.",
        default=BASE_DATA_PATH,
    )
    args = parser.parse_args()

    sns.set(style="darkgrid")
    plotter = Plotter(
        [
            JobWaitingTimeMetric,
            JobMakespanMetric,
            JobTurnaroundTimeMetric,
            TaskThroughputMetric,
            PowerConsumptionMetric,
            IdleTimeMetric
        ],
        {
            JobWaitingTimeMetric: MetricWorkloadBarPlot,
            JobMakespanMetric: MetricWorkloadBarPlot,
            JobTurnaroundTimeMetric: MetricWorkloadBarPlot,
            TaskThroughputMetric: MetricWorkloadBarPlot,
            PowerConsumptionMetric: MetricWorkloadBarPlot,
            IdleTimeMetric: MetricWorkloadBarPlot
        },
        # ["power-consumption", "turnaround-time", "idle-time", "task-throughput", "utilisation"],
        # {
        #     "power-consumption": "Power Consumption (watts)",
        #     "turnaround-time": "Turnaround time (seconds)",
        #     "idle-time": "Idle time (in seconds)",
        #     "task-throughput": "Task throughput (tasks per second)",
        #     "utilisation": "Utilisation (%)",
        # },
        args.path
    )
    plotter.plot_all()


if __name__ == "__main__":
    main()
