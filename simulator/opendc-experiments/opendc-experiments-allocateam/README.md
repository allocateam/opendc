# Allocateam experiments

The goal of the Allocateam experiments is to analyze and compare different resource allocation policies.

## Setup

The `tools` folder contains the necessary helper scripts that make it possible to download the necessary traces and
easily run the experiments from the command-line.

In order to make your life easier, make sure you add the execution permission to these scripts by running:
```shell
$ chmod +x ./tools/*.sh
```

Before being able to run the experiments, the traces need to be downloaded. This can be done with the `setup.sh` script,
by running:
```shell
$ ./tools/setup.sh
```

After running this command, the traces (downloaded from the Workflow Trace Archive) will be stored under `src/main/resources/traces`.

## Running the experiments

:fire: :fire: ***TL;DR:* Make sure to follow the instructions in the "Setup" section first!** :fire: :fire:

To run the experiments, just execute the following command:
```shell
$ ./tools/run.sh smokeTest
```

This will compile & run the code necessary for running the experiments. The results can be found under the `data` directory in the Parquet format.
