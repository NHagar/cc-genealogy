#!/bin/bash

# Define list of directories to check
directories=(
    "data/results/c4_en"
    "data/results/cultura"
    "data/results/falcon"
    # "data/results/fineweb"
    # "data/results/fineweb_edu"
    # "data/results/madlad_cleaned"
    # "data/results/zyda_2"
    # "data/results/dclm"
    # "data/results/dolma"
)

# Define required files to check in each directory
required_files=(
    "topline.csv"
    "domains.csv"
    "casestudies.csv"
)

# Initialize array to store job IDs for each dataset
declare -A job_ids

# Loop through directories
for dir in "${directories[@]}"; do
    echo "Checking directory: $dir"
    # Extract dataset name (last part of directory path)
    dataset=$(basename "$dir")
    job_ids[$dataset]=""
    
    for req_file in "${required_files[@]}"; do
        if [ -f "$dir/$req_file" ]; then
            echo "  $req_file: yes"
        else
            # Extract analysis step (part before .csv)
            analysis_step=${req_file%.csv}
            echo "  $req_file: no - submitting pipeline job..."
            # Submit job and capture job ID
            job_id=$(sbatch --parsable ./analysis_pipeline.sh "$dataset" "$analysis_step")
            # Append to job IDs for this dataset, separated by colons
            if [ -z "${job_ids[$dataset]}" ]; then
                job_ids[$dataset]="$job_id"
            else
                job_ids[$dataset]="${job_ids[$dataset]}:$job_id"
            fi
            echo "  Submitted job $job_id"
        fi
    done
    echo "-------------------"
done

# Submit sample analysis jobs with dependencies
for dir in "${directories[@]}"; do
    if [ -f "$dir/domains.csv" ]; then
        dataset=$(basename "$dir")
        echo "Found domains.csv in $dir - submitting sample analysis..."
        # If there are previous jobs for this dataset, wait for them
        if [ -n "${job_ids[$dataset]}" ]; then
            sbatch --dependency=afterok:${job_ids[$dataset]} ./analysis_pipeline.sh "$dataset" "sample"
        else
            sbatch ./analysis_pipeline.sh "$dataset" "sample"
        fi
    fi
done