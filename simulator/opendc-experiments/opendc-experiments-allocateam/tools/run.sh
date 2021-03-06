#!/bin/bash
# This script runs the Allocateam experiment.
# Usage: ./tools/run.sh smokeTest

set -euo pipefail

EXPERIMENT_DIR="$( cd "$(dirname "$0")/../" > /dev/null 2>&1 ; pwd -P )"
TRACES_DIR="$EXPERIMENT_DIR/src/main/resources/traces"
DATA_DIR="$EXPERIMENT_DIR/data"
GRADLE="$( cd "$(dirname "$0")/../../../" > /dev/null 2>&1 ; pwd -P )/gradlew"

# Define realpath for platforms that don't support a command line version by default (i.e. macOS)
command -v realpath > /dev/null 2>&1 || realpath() {
	[[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

# Check if data directory exists and ask user what to do as we don't want to remove data
if [ -d "$DATA_DIR" ]; then
	while getopts ":y" opt; do # if -y option is given, remove data dir.
		case $opt in
			y) echo "Removing data directory.." && rm -r "$DATA_DIR" && mkdir "$DATA_DIR";;
		esac
		done
	if [ $OPTIND -eq 1 ]; then  # if no options given, prompt for data dir removal.
		read -rp "data directory exists. Remove it (y/n)? " choice
		case "$choice" in
			y|Y ) rm -r "$DATA_DIR" && mkdir "$DATA_DIR";;
			n|N ) echo "Cannot proceed without (re)moving data directory. Aborting..."; exit 1;;
			* ) echo "Ambiguous answer. Aborting..."; exit 2;;
		esac
	fi
else
	mkdir "$DATA_DIR"
fi

"$GRADLE" :opendc-experiments:opendc-experiments-allocateam:run --args="--trace-path \"$TRACES_DIR\" --portfolio \"$1\" -O \"$DATA_DIR\""
