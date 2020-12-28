from abc import ABC, abstractmethod

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# HACK(gm): ignore warnings by matplotlib regarding FixedFormatter for ticks
import warnings
warnings.filterwarnings("ignore")

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


class Plot(ABC):
    @abstractmethod
    def generate(self, data: pd.DataFrame, metric, plotter, x_axis_label):
        pass


class MetricWorkloadPlot(Plot):
    def __init__(self):
        self.method = None
        self.postfix_path = ""

    def generate(self, data, metric, plotter, x_axis_label):
        for topology in data.topology.unique():
            dir_path = f'{plotter.OUTPUT_PATH}/{metric.name}/topology-{topology}'
            plotter.make_output_path(dir_path)

            for workload in data.workload.unique():
                plt.figure(figsize=(10, 5))
                data['workload-topology'] = data.workload + " / " + data.topology
                g = self.method(
                    data=data[(data.workload == workload) & (data.topology == topology)],
                    x=metric.name,
                    y="workload-topology",
                    hue="allocation_policy",
                    ci=None,
                )

                xlabels = [reformat_large_tick_values(x) for x in g.get_xticks()]
                g.set_xticklabels(xlabels)

                g.set_xlabel(x_axis_label)
                g.set_ylabel("Workload")
                plt.yticks(rotation=90, va="center")
                plt.legend(title="Allocation policy", bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

                plt.tight_layout()
                postfix = f"-{self.postfix_path}" if self.postfix_path else ""
                plt.savefig(f'{dir_path}/{workload}{postfix}.png')


class MetricWorkloadBarPlot(MetricWorkloadPlot):
    def __init__(self):
        super().__init__()
        self.method = sns.barplot


class MetricWorkloadViolinPlot(MetricWorkloadPlot):
    def __init__(self):
        super().__init__()
        self.method = sns.violinplot
        self.postfix_path = "violin"
