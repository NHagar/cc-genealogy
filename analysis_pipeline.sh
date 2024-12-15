#!/bin/bash

# Check if dataset argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <dataset_path>"
    exit 1
fi

# Store dataset path from command line argument
DATASET=$1

# Execute DuckDB query
# Replace the SELECT query with your specific query
duckdb  :memory: "COPY (SELECT url FROM read_parquet('$DATASET') LIMIT 5) TO 'test.csv';"

# Exit with success status
exit 0