#!/bin/bash
# Executes given cargo command across all feature sets

# Check if a command is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <cargo-args...>"
    exit 1
fi

# Define the feature sets
feature_sets=(
    "std"
    "std,sync"
    "std,sync,multi-threaded"
    "std,multi-threaded"
    "sync"
    "sync,multi-threaded"
    "multi-threaded"
    ""
)

# Extract arguments before and after '--'
args_before_dash=()
args_after_dash=()
found_dash=0

for arg in "$@"; do
    if [ "$arg" = "--" ]; then
        found_dash=1
        continue
    fi
    if [ "$found_dash" -eq 0 ]; then
        args_before_dash+=("$arg")
    else
        args_after_dash+=("$arg")
    fi
done

# Loop through each feature set and execute the cargo command
for features in "${feature_sets[@]}"; do
    echo "Running cargo $@ with features: ${features:-none}"

    cargo_cmd=(cargo "${args_before_dash[@]}" --no-default-features)
    if [ -n "$features" ]; then
        cargo_cmd+=("--features" "$features")
    fi

    if [ "$found_dash" -eq 1 ]; then
        cargo_cmd+=("--" "${args_after_dash[@]}")
    fi

    "${cargo_cmd[@]}"
done
