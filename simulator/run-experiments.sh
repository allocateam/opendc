#!/bin/bash

set -euo pipefail

# Define realpath for platforms that don't support a command line version by default (i.e. macOS)
command -v realpath > /dev/null 2>&1 || realpath() {
	[[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

OUTPUT_PATH="$(pwd)/data"
rm -rf "$OUTPUT_PATH"

./gradlew :opendc-experiments:opendc-experiments-sc20:run --args="--environment-path \"$(realpath "$1")\" --trace-path \"$(realpath "$2")\" --portfolio $3 --output \"$OUTPUT_PATH\""
