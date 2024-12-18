#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=4:00:00
#SBATCH --mem-per-cpu=8G
#SBATCH --job-name=url-analysis

# Check if both arguments are provided
if [ $# -ne 2 ]; then
    echo "Error: Please provide two arguments:"
    echo "1. dataset name"
    echo "2. analysis step"
    exit 1
fi

# Store arguments
dataset=$1
analysis_step=$2

echo "Processing dataset: $dataset"
echo "Running analysis step: $analysis_step"

# Define dataset file patterns
declare -A dataset_patterns
dataset_patterns["c4_en"]="hf://datasets/nhagar/c4_en_urls/data/*.parquet"
dataset_patterns["dataset2"]="data/raw/dataset2/*.csv"
dataset_patterns["dataset3"]="data/processed/dataset3/*.tsv"

# Define SQL query paths based on analysis steps
declare -A sql_queries
sql_queries["topline"]="queries/analysis/top_level_count.sql"
sql_queries["domains"]="queries/analysis/domain_counts.sql"
sql_queries["sample"]="queries/analysis/stratified_sample.sql"
sql_queries["casestudies"]="queries/analysis/case_studies.sql"

# Verify dataset exists in patterns
if [ -z "${dataset_patterns[$dataset]}" ]; then
    echo "Error: Unknown dataset '$dataset'"
    exit 1
fi

# Verify analysis step exists and set query path
if [ -z "${sql_queries[$analysis_step]}" ]; then
    echo "Error: Unknown analysis step '$analysis_step'"
    exit 1
fi

data_pattern="${dataset_patterns[$dataset]}"
sql_query_path="${sql_queries[$analysis_step]}"
output_file="data/results/$dataset/$analysis_step.csv"

# Create output directory if it doesn't exist
mkdir -p "$(dirname "$output_file")"

# Read SQL query and replace variables
sql_query=$(<"$sql_query_path")
sql_query="${sql_query//DATASET_PATTERN/$data_pattern}"
sql_query="${sql_query//OUTPUT_PATH/$output_file}"

# Execute the processed query
echo "$sql_query" | duckdb
