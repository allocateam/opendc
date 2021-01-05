# Allocateam experiments

The goal of the Allocateam experiments is to analyze and compare different resource allocation policies.

## Setup

The `tools` folder contains the necessary helper scripts that make it possible to download the necessary traces and
easily run the experiments from the command-line.


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


### Plotting the results

To plot the results, we use the Python module `plot.py`. This will plot the `.parquet` file with the metrics.

#### Environment

The `Dockerfile` already contains the necessary requirements in `pip`. Otherwise, the requirements can be obtained from `requirements.txt` using
`pip3 install -r requirements.txt`.

#### Instructions

To plot, run the following:

```bash
# Defaults to data/metrics.parquet
python3 plot.py 

# or provide your own metrics file
python3 plot.py <path_to_parquet>
```

The plots can be found in `tools/plot/plots/`.

### Viewing the results as a CSV (Optional)

After running the script a folder called `data` should be created in the `opendc-experiments-allocateam` directory. The folder contains experiment results
in the parquet format. To view parquet files you can install *parquet_tools* to create a CSV.

##### Dev container

Create a CSV:
```bash
parquet-tools csv data/experiments.parquet > ./data/experiments.csv
```

##### mac OS
`brew install parquet-tools`

Create a JSON:
```shell script
parquet-tools cat --json data/experiments.parquet > ./data/experiments.json
```

## Running tests

```bash
# Inside opendc/simulator/opendc-experiments/opendc-experiments-allocateam
../../gradlew clean test
```

## Verify repeatability

> [verify_repeatability.py] aims to verify the repeatability of the Allocateam experiment,
by first running the experiment three times in setting 3 (medium topology, spec_trace 2, all metrics).
We then assert that the metrics produced by the three runs are equivalent.

To run the `verify_repeatability.py` script:

```bash
# Run the experiment 
# (setting 3, with 3 allegedly identical runs that will be verified on their equality)
./tools/run.sh verifyRepeatability

# Run the verify repeatability script
python3 ./tools/plot/verify_repeatability.py data/
```
