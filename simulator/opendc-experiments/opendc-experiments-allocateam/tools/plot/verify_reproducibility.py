"""This module aims to verify the reproducibility of the Allocateam experiment, 
by first running the experiment three times in setting 3 (medium topology, spec_trace 2, all metrics).
We then assert that the metrics produced by the three runs are equivalent.
"""

import argparse
import os.path as path
import shlex
import subprocess
from pathlib import Path
from typing import List, Type, Dict

import metrics
import pandas as pd
from metrics import Metric, Plot
import plot


def collect_metrics(data_path: Path, metric_classes: List[Type[Metric]]) -> Dict[int, List[pd.DataFrame]]:
    """Collect metrics per run.

    Args:
        data_path (Path): Path to the metrics.
        metric_classes (List[Type[Metric]]): List of metrics to collect.

    Returns:
        Dict[int, List[pd.DataFrame]]: Key is run id. Value is list of metrics per scenario.
    """
    experiments = pd.read_parquet(f"{data_path}/experiments.parquet")
    
    scenarios_by_run: dict = {}
    for scenario in plot.iter_runs(experiments):
        if not scenarios_by_run.get(scenario.run_id):
            scenarios_by_run[scenario.run_id] = []
        scenarios_by_run[scenario.run_id].append(scenario)

    metrics_by_run: dict = {}
    for run, scenarios in scenarios_by_run.items():
        for metric in metric_classes:
            df = metric([], scenarios).metric_dataframe()
            if not metrics_by_run.get(run):
                metrics_by_run[run] = []
            metrics_by_run[run].append(df)

    return metrics_by_run

def assert_equality(metrics_per_run: Dict[int, List[pd.DataFrame]]):
    """Assert that the metrics for each run are the same.

    Args:
        metrics_per_run (Dict[int, List[pd.DataFrame]]): The metrics per run.
    """
    print("Verifying that each run returns equal results.")
    for run_id_a, run_metrics_a in metrics_per_run.items():
        for run_id_b, run_metrics_b in metrics_per_run.items():
            if run_id_a == run_id_b: continue
            for i in range(len(run_metrics_a)):
                print(f"Asserting equality for run {run_id_a} and run {run_id_b}")
                print(f"For the metric: {run_metrics_a[i].columns[-1]}")
                assert list(run_metrics_a[i]) == list(run_metrics_b[i])
    print("All runs are equal!")

def main():
    parser = argparse.ArgumentParser(description="Verify reproducibility for the Allocateam experiment.")
    parser.add_argument(
        "path",
        nargs='?',
        type=str,
        help="The path to data dir.",
        default=metrics.metric.BASE_DATA_PATH,
    )
    args = parser.parse_args()

    all_metrics = [
        metrics.JobTurnaroundTimeMetric,
        metrics.TaskThroughputMetric,
        metrics.PowerConsumptionMetric,
        metrics.IdleTimeMetric,
        metrics.JobWaitingTimeMetric,
        metrics.JobMakespanMetric,
    ]

    metrics_per_run = collect_metrics(args.path, all_metrics)
    assert_equality(metrics_per_run)
    

if __name__ == "__main__":
    """Usage: python3 verify_reproducibility.py <path_to_data_dir>"""
    main()
