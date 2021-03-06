"""This module aims to verify the repeatability of the Allocateam experiment,
by first running the experiment three times in setting 3 (medium topology, spec_trace 2, all metrics).
We then assert that the metrics produced by the three runs are equivalent.
"""

import argparse
from pathlib import Path
from typing import List, Type, Dict

import metrics
import pandas as pd
from metrics import Metric
import plot


def collect_metrics_per_run(data_path: Path, metric_classes: List[Type[Metric]]) -> Dict[int, List[pd.DataFrame]]:
    """Collect metrics per run.

    Args:
        data_path (Path): Path to the metrics.
        metric_classes (List[Type[Metric]]): List of metrics to collect.

    Returns:
        Dict[int, List[pd.DataFrame]]: Key is run id. Value is list of metrics per scenario.
    """
    experiments = pd.read_parquet(f"{data_path}/experiments.parquet")

    scenarios_per_run: dict = {}
    for scenario in plot.iter_runs(experiments):
        if not scenarios_per_run.get(scenario.run_id):
            scenarios_per_run[scenario.run_id] = []
        scenarios_per_run[scenario.run_id].append(scenario)

    metrics_per_run: dict = {}
    for run_id, scenarios in scenarios_per_run.items():
        for metric in metric_classes:
            df = metric([], scenarios).metric_dataframe()
            if not metrics_per_run.get(run_id):
                metrics_per_run[run_id] = []
            metrics_per_run[run_id].append(df)

    return metrics_per_run


def assert_equality(metrics_per_run: Dict[int, List[pd.DataFrame]]):
    """Assert that the metrics for each run are the same.

    Args:
        metrics_per_run (Dict[int, List[pd.DataFrame]]): The metrics per run.
    """
    print("Verifying that each run returns equal results.")
    for run_id_a, run_metrics_a in metrics_per_run.items():
        for run_id_b, run_metrics_b in metrics_per_run.items():
            if run_id_a == run_id_b:
                continue
            for i in range(len(run_metrics_a)):
                print(f"Asserting equality for run {run_id_a} and run {run_id_b}")
                print(f"For the metric: {run_metrics_a[i].columns[-1]}")
                assert list(run_metrics_a[i]) == list(run_metrics_b[i])
    print("All runs are equal!")


def main():
    parser = argparse.ArgumentParser(description="Verify repeatability for the Allocateam experiment.")
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

    metrics_per_run = collect_metrics_per_run(args.path, all_metrics)
    assert_equality(metrics_per_run)


if __name__ == "__main__":
    """Usage: python3 verify_repeatability.py <path_to_data_dir>"""
    main()
