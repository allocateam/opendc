#!/bin/bash

OUTPUT_PATH="$(pwd)/data"
rm -rf "$OUTPUT_PATH"
mkdir "$OUTPUT_PATH"

./gradlew :opendc-experiments:opendc-experiments-allocateam:run --args=\
"--environment-path \"$(realpath $1)\" --trace-path \"$(realpath $2)\" --portfolio $3 --output \"$OUTPUT_PATH\""
