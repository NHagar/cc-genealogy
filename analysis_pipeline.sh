#!/bin/bash

# Check if dataset argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <dataset_path>"
    exit 1
fi

# Store dataset path from command line argument
DATASET=$1

# Top-level count
duckdb  :memory: "COPY (SELECT url FROM read_parquet('$DATASET') LIMIT 5) TO 'test_count.csv';"

# Create temporary table with domain counts
DOMAIN_COUNTS="domain_counts.csv"
duckdb :memory: "COPY (SELECT split_part(url, '/', 3) as domain, COUNT(*) as count FROM read_parquet('$DATASET') GROUP BY domain ORDER BY count DESC) TO '$DOMAIN_COUNTS';"

# Run stratified sample with parameters
SAMPLE_OUTPUT="stratified_sample.csv"
duckdb :memory: -init queries/analysis/stratified_sample.sql "$DOMAIN_COUNTS" "$SAMPLE_OUTPUT"

# Cleanup
rm "$DOMAIN_COUNTS"

# Case study search
duckdb  :memory: "COPY (SELECT url FROM read_parquet('$DATASET') WHERE CONTAINS(url, 'nytimes.com') LIMIT 5) TO 'test_nyt.csv';"

# Exit with success status
exit 0