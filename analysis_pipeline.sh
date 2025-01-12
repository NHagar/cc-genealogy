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

# Get dataset pattern
get_dataset_pattern() {
    case "$1" in
        "c4_en")
            echo "hf://datasets/nhagar/c4_en_urls/data/*.parquet"
            ;;
        "falcon")
            echo "hf://datasets/nhagar/falcon_urls/data/*.parquet"
            ;;
        "cultura")
            echo "hf://datasets/nhagar/cultura_urls/data/*.parquet"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Get SQL query path
get_sql_query_path() {
    case "$1" in
        "topline")
            echo "queries/analysis/top_level_count.sql"
            ;;
        "domains")
            echo "queries/analysis/domain_counts.sql"
            ;;
        "sample")
            echo "queries/analysis/stratified_sample.sql"
            ;;
        "casestudies")
            echo "queries/analysis/case_studies.sql"
            ;;
        *)
            echo ""
            ;;
    esac
}

data_pattern=$(get_dataset_pattern "$dataset")
sql_query_path=$(get_sql_query_path "$analysis_step")

# Special handling for sample analysis
if [ "$analysis_step" = "sample" ]; then
    data_pattern="data/results/$dataset/domains.csv"
fi

# Verify dataset pattern exists
if [ -z "$data_pattern" ]; then
    echo "Error: Unknown dataset '$dataset'"
    exit 1
fi

# Verify SQL query path exists
if [ -z "$sql_query_path" ]; then
    echo "Error: Unknown analysis step '$analysis_step'"
    exit 1
fi

output_dir="data/results/$dataset"
output_file="$output_dir/$analysis_step.csv"

# Create output directory if it doesn't exist
mkdir -p "$(dirname "$output_file")"

# Read SQL query and replace variables
sql_query=$(<"$sql_query_path")
sql_query="${sql_query//DATASET_PATTERN/$data_pattern}"
sql_query="${sql_query//OUTPUT_PATH/$output_file}"

echo "Executing SQL query:"
echo "$sql_query"

# Execute the processed query
echo "$sql_query" | duckdb
