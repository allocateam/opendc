#!/usr/bin/env bash
# This script downloads the traces.
# Usage: ./tools/setup.sh

tracesDir="$( cd "$(dirname "$0")/../" > /dev/null 2>&1 || exit ; pwd -P )/src/main/resources/traces"

tracesUrls=(
	"https://zenodo.org/record/3254576/files/shell_parquet.zip?download=1"
	"https://zenodo.org/record/3254471/files/askalon_ee_parquet.zip?download=1"
	"https://zenodo.org/record/3254594/files/spec_trace-2_parquet.zip?download=1"
)

echo "Downloading traces..."
for url in "${tracesUrls[@]}"; do
	filename="$tracesDir/$(echo -n "$url" | cut -d '/' -f 7 | cut -d '?' -f 1)"
	curl -o "$filename" "$url"
done

echo "Extracting traces..."
for url in "${tracesUrls[@]}"; do
	filename="$tracesDir/$(echo -n "$url" | cut -d '/' -f 7 | cut -d '?' -f 1)"
	directory="$(dirname "$filename")"
	unzip "$filename" -d "$directory" > /dev/null
	mv "$directory/$(basename "$filename" .zip)" "$directory/$(basename "$filename" _parquet.zip)"
	rm "$filename"
done
