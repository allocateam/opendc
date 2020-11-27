## Distributed Systems Lab - Compare Datacenter Scheduling Policies

The original README for OpenDC which includes an overview of the architecture as well as how to set it up and more details can be found [here](original_readme.md).

### Branching policy

The `main` branch is used for the work that is done in the context of this project. It is based on the master branch of the official OpenDC repository. As such it should be suitable for merging into it, if/once the need arises to do so. However, before doing so, it should be checked which files have been created/modified in order to make the work of implementing & testing different allocation policies possible before attempting to do a merge with the official OpenDC `master` branch, as some files maybe be irrelevant for pushing changes back upstream.

The `old-master` is a copy of the original `master` branch and is kept as a reference.

### Running experiments

There is shell script in the simulator folder that allows you to easily run experiments. To run script:

### (Optional) Create a dev container

```bash
# Build container
docker build -t opendc-dev .
```

```bash
# Start container
docker run -dit -v /path/to/local/opendc:/home/opendc/ --name opendc-dev opendc-dev:latest bash
```

```bash
# Get into container's shell
docker exec -it opendc-dev bash
```

#### Navigate to simulator folder

```shell script
cd simulator
```

#### Run script

```shell script
./run-experiments.sh ./ds_exp/mock/env ./ds_exp/mock/traces test
```
The arguments have the following meaning:
- `./ds_exp/mock/env`: Path to experiment environment
- `./ds_exp/mock/traces`: Path to experiment traces
- `test`: Portfolio

#### View results

After running the script a folder called `data` should be created in the current directory. The folder contains experiment results
in the parquet format. 

To view parquet files you can install *parquet_tools*

##### Dev container

To view experiment results:
```bash
parquet-tools csv data/experiments.parquet > ./data/experiments.csv
```

##### mac OS
`brew install parquet-tools`

To view experiment results:
```shell script
parquet-tools cat --json data/experiments.parquet > ./data/experiments.json
```
