from abc import ABC, abstractmethod
from typing import Type, List
from .plot import Plot
import pandas as pd

from pathlib import Path

BASE_DATA_PATH = (Path(__file__).parent / "../../../data").resolve()


def metric_path(name, scenario):
    partition = "portfolio_id={}/scenario_id={}/run_id={}".format(
        scenario.portfolio_id, scenario.scenario_id, scenario.run_id
    )
    return Path(BASE_DATA_PATH) / name / partition / "data.parquet"


class Metric(ABC):
    def __init__(self, plots: List[Type[Plot]], scenarios):
        self.name = "metric"
        self.plots = plots
        self.scenarios = scenarios
        self.x_axis_label = "no label"
        self.df_cache = None

    def metric_dataframe(self) -> pd.DataFrame:
        result = []
        for scenario in self.scenarios:
            for value in self.get_data(scenario):
                result.append({
                    "portfolio_id": scenario.portfolio_id,
                    "topology": scenario.topology,
                    "workload": scenario.workload_name,
                    "allocation_policy": scenario.allocation_policy,
                    self.name: value,
                })
        return pd.DataFrame.from_dict(result)

    def generate_plot(self, plotter):
        if self.df_cache is None:
            self.df_cache = self.metric_dataframe()
        for plot in self.plots:
            plot().generate(self.df_cache, self, plotter, self.x_axis_label)

    @abstractmethod
    def get_data(self, scenario):
        pass
