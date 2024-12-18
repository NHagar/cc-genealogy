#!/bin/bash

# Define list of directories to check
directories=(
    "data/results/c4_en"
    "data/results/cultura"
    "data/results/falcon"
    "data/results/fineweb"
    "data/results/fineweb_edu"
    "data/results/madlad_cleaned"
    "data/results/zyda_2"
    "data/results/dclm"
    "data/results/dolma"
)

# Define required files to check in each directory
required_files=(
    "topline.csv"
    "domains.csv"
    "sample.csv"
    "casestudies.csv"
)

# Loop through directories
for dir in "${directories[@]}"; do
    echo "Checking directory: $dir"
    # Extract dataset name (last part of directory path)
    dataset=$(basename "$dir")
    
    for req_file in "${required_files[@]}"; do
        if [ -f "$dir/$req_file" ]; then
            echo "  $req_file: yes"
        else
            # Extract analysis step (part before .csv)
            analysis_step=${req_file%.csv}
            echo "  $req_file: no - running pipeline..."
            ./analysis_pipeline.sh "$dataset" "$analysis_step"
        fi
    done
    echo "-------------------"
done