#!/bin/bash

OUTPUT_PATH="$(pwd)/data"
rm -rf "$OUTPUT_PATH"

./gradlew :opendc-experiments:opendc-experiments-sc20:run --args=\
"--environment-path \"$(realpath $1)\" --trace-path \"$(realpath $2)\" --portfolio $3 --output \"$OUTPUT_PATH\""
