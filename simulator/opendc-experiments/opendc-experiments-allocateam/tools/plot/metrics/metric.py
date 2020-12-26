from abc import ABC, abstractmethod
from typing import Type
from .plot import Plot
import pandas as pd

from pathlib import Path

BASE_DATA_PATH = (Path(__file__).parent / "../../../data").resolve()


def metric_path(name, run):
    partition = "portfolio_id={}/scenario_id={}/run_id={}".format(
        run.portfolio_id, run.scenario_id, run.run_id
    )
    return Path(BASE_DATA_PATH) / name / partition / "data.parquet"


class Metric(ABC):
    def __init__(self, plot: Type[Plot], runs):
        self.name = "metric"
        self.plot = plot
        self.runs = runs
        self.x_axis_label = "no label"

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
        self.plot().generate(self.metric_dataframe(), self, plotter, self.x_axis_label)

    @abstractmethod
    def get_data(self, run):
        pass
