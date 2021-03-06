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
    new_tick_format = tick_val

    sign = '-' if tick_val < 0 else ''
    tick_abs = abs(tick_val)

    if tick_abs >= 1000000000:
        val = round(tick_abs / 1000000000, 1)
        new_tick_format = '{}{:}B'.format(sign, val)
    elif tick_abs >= 1000000:
        val = round(tick_abs / 1000000, 1)
        new_tick_format = '{}{:}M'.format(sign, val)
    elif tick_abs >= 1000:
        val = round(tick_abs / 1000, 1)
        new_tick_format = '{}{:}K'.format(sign, val)
    elif 0 < tick_abs < 1:
        new_tick_format = '{}{:.2e}'.format(sign, tick_abs)
    elif tick_abs < 1000:
        new_tick_format = '{}{}'.format(sign, round(tick_val, 1))

    # make new_tick_format into a string value
    new_tick_format = str(new_tick_format)

    # code below will keep 4.5M as is but change values such as 4.0M to 4M
    # since that zero after the decimal isn't needed
    index_of_decimal = new_tick_format.find(".")

    if index_of_decimal != -1:
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
                plt.savefig(f'{dir_path}/{workload}{postfix}.svg')


class MetricWorkloadBarPlot(MetricWorkloadPlot):
    def __init__(self):
        super().__init__()
        self.method = sns.barplot


class MetricWorkloadViolinPlot(MetricWorkloadPlot):
    def __init__(self):
        super().__init__()
        self.method = sns.violinplot
        self.postfix_path = "violin"


class ReportSetting1(Plot):
    def __init__(self):
        super().__init__()
        self.filename = ""
        self.method = None
        self.row = None

    def generate(self, data, metric, plotter, x_axis_label):
        dir_path = f'{plotter.OUTPUT_PATH}/report'
        plotter.make_output_path(dir_path)

        data['workload-topology'] = data.workload + " / " + data.topology

        g = sns.FacetGrid(
            data,
            row=self.row,
            margin_titles=False,
            aspect=2,
            height=4,
            sharex=False
        )
        g.map(self.method, metric.name, "allocation_policy", ci=None, palette="Set1")
        g.set_titles("")
        g.set_axis_labels("")
        labels = list(data['workload-topology'].unique())
        for idx, axis in enumerate(g.axes.flatten()):
            xlabels = [reformat_large_tick_values(x) for x in axis.get_xticks()]
            axis.set_xticklabels(xlabels)
            axis.set_ylabel(labels[idx])

        plt.tight_layout()
        plt.savefig(f'{dir_path}/{self.filename}.pdf')


class ReportSetting1Makespan(ReportSetting1):
    def __init__(self):
        super().__init__()
        self.filename = "s1-makespan"
        self.method = sns.barplot
        self.row = "topology"


class ReportSetting1WaitingTime(ReportSetting1):
    def __init__(self):
        super().__init__()
        self.filename = "s1-waiting-violin"
        self.method = sns.violinplot
        self.row = "topology"


class ReportSetting2Makespan(ReportSetting1):
    def __init__(self):
        super().__init__()
        self.filename = "s2-makespan"
        self.method = sns.barplot
        self.row = "workload"


class ReportSetting2WaitingTime(ReportSetting1):
    def __init__(self):
        super().__init__()
        self.filename = "s2-waiting-violin"
        self.method = sns.violinplot
        self.row = "workload"


class ReportSetting3(Plot):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename

    def generate(self, data, _, plotter, x_axis_labels):
        dir_path = f'{plotter.OUTPUT_PATH}/report'
        plotter.make_output_path(dir_path)

        # get dataframes and merge them
        data['workload-topology'] = data.workload + " / " + data.topology

        g = sns.FacetGrid(
            data,
            row="metric",
            margin_titles=False,
            aspect=2,
            height=4,
            sharex=False
        )
        g.map(sns.barplot, "value", "allocation_policy", ci=None, palette="Set1")
        g.set_axis_labels("")
        titles = {
            "metric = job_makespan": "Job makespan (seconds)",
            "metric = job_waiting_time": "Job waiting time (seconds)",
            "metric = idle_time": "Average idle percentage (per machine)",
            "metric = job_turnaround": "Turnaround time (seconds)",
            "metric = power_consumption": "Power Consumption (watt-hours)",
            "metric = task_throughput": "Task throughput (tasks per hour)"
        }
        labels = list(data['workload-topology'].unique())
        for idx, axis in enumerate(g.axes.flatten()):
            xlabels = [reformat_large_tick_values(x) for x in axis.get_xticks()]
            axis.set_xticklabels(xlabels)
            axis.set_ylabel(labels[0])
            axis.set_title(titles.get(axis.title.get_text(), "Missing label"))

        plt.tight_layout()
        plt.savefig(f'{dir_path}/{self.filename}.pdf')
