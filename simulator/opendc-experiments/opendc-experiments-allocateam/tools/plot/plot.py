import matplotlib.pyplot as plt; plt.rcdefaults()
import argparse
import os
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class Plotter():
    OUTPUT_PATH = f"{os.path.dirname(__file__)}/plots/{datetime.now()}"

    def __init__(self, path: str):
        self.data = pd.read_csv(path)
        self._make_output_path()

    def _make_output_path(self):
        Path(self.OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

    def plot_all(self):
        print("Plotting..")
        self._plot_column('overcommitted_cpu_cycles', unit="MFLOP")
        self._plot_column('total_power_consumption')
        self._plot_column('total_num_failed_vm_slices')
        self._plot_column('granted_cpu_cycles')
        self._plot_column('interfered_cpu_cycles')
        self._plot_column('requested_cpu_cycles')
        self._plot_column('mean_host_cpu_usage')
        self._plot_column('mean_host_cpu_demand')
        self._plot_column('mean_num_deployed_images_per_host')
        self._plot_column('max_num_deployed_images_per_host')
        self._plot_column('total_num_vms_submitted')
        self._plot_column('max_num_vms_submitted')
        self._plot_column('max_num_vms_finished')
        self._plot_column('max_num_vms_failed')
        print(f"Plots successfully stored in {self.OUTPUT_PATH}")

        
    def _plot_column(self, column: str, unit: str = None):
        self.data.plot.barh(x='scenario_id', y=column, )
        plt.ylabel("Allocation policy")
        plt.xlabel(unit)
        output_file_path = f'{self.OUTPUT_PATH}/{column}.png'
        plt.savefig(output_file_path)


def main():
    """Usage: python3 plot.py <path_to_csv>
    See example.csv for an example of the input data.
    """
    parser = argparse.ArgumentParser(description="Plot metrics for the Alocateam experiment.")
    parser.add_argument(
        "path",
        type=str,
        help="The path to the input csv file.",
    )
    args = parser.parse_args()

    plotter = Plotter(args.path)
    plotter.plot_all()

if __name__ == "__main__":
    main()
