#!/usr/bin/env python3

import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


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


def aggregate_power_consumption(df):
    return df.agg({"wattage": ['mean']}).wattage[0]


def aggregate_duration(df):
    return df.agg({"duration": ['mean']}).duration[0]


def aggregate_task_throughput(df):
    return df.tasks_per_second[0]


def aggregate_utilisation(df):
    return df.agg({"utilisation": ['mean']}).utilisation[0] * 100


class Plotter:
    OUTPUT_PATH = f"{Path(__file__).parent.resolve()}/results/{datetime.now()}"

    def __init__(self, metrics: List[str], labels: Dict[str, str], path: str):
        self.metrics = metrics
        self.labels = labels
        self.path = path

        experiments = pd.read_parquet(path / "experiments.parquet")
        self.data = self._preprocess(experiments)
        self._make_output_path()

    def _make_output_path(self):
        Path(self.OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

    def _preprocess(self, experiments):
        aggregation_funcs = {
            "power-consumption": aggregate_power_consumption,
            "turnaround-time": aggregate_duration,
            "idle-time": aggregate_duration,
            "task-throughput": aggregate_task_throughput,
            "utilisation": aggregate_utilisation
        }

        results = {
            metric: []
            for metric in self.metrics
        }

        metric_path_fmt = "portfolio_id={}/scenario_id={}/run_id={}"

        for metric, aggregation_func in aggregation_funcs.items():
            for portfolio_id in experiments['portfolio_id'].unique():
                for scenario_id in experiments['scenario_id'].unique():
                    p_id = experiments['portfolio_id'] == portfolio_id
                    s_id = experiments['scenario_id'] == scenario_id
                    for _, run in experiments[p_id & s_id].iterrows():
                        run_path = metric_path_fmt.format(run.portfolio_id, run.scenario_id, run.run_id)
                        metric_path = self.path / metric / run_path / "data.parquet"
                        df = pd.read_parquet(metric_path)
                        results[metric].append({
                            "topology": run.topology,
                            "workload": run.workload_name,
                            "allocation_policy": run.allocation_policy,
                            metric: aggregation_func(df)
                        })

        return results

    def plot_all(self):
        print("Plotting..")
        for metric in self.metrics:
            self._plot_metric(metric)

        # self._plot_column('task_throughput', unit="tasks per second")
        # self._plot_column('turnaround_time', unit="time")
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

    default_metrics_location = Path(__file__).parent / "../../data"
    parser = argparse.ArgumentParser(description="Plot metrics for the Allocateam experiment.")
    parser.add_argument(
        "path",
        nargs='?',
        type=str,
        help="The path to the input csv file.",
        default=default_metrics_location.resolve(),
    )
    args = parser.parse_args()

    sns.set(style="darkgrid")
    plotter = Plotter(
        ["power-consumption", "turnaround-time", "idle-time", "task-throughput", "utilisation"],
        {
            "power-consumption": "Power Consumption (watts)",
            "turnaround-time": "Turnaround time (seconds)",
            "idle-time": "Idle time (in seconds)",
            "task-throughput": "Task throughput (tasks per second)",
            "utilisation": "Utilisation (%)",
        },
        args.path
    )
    plotter.plot_all()


if __name__ == "__main__":
    main()
