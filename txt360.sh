#!/bin/bash

# Define the list of snapshots
snapshots=(
    "CC-MAIN-2013-20"
    "CC-MAIN-2013-48"
    "CC-MAIN-2014-10"
    "CC-MAIN-2014-15"
    "CC-MAIN-2014-23"
    "CC-MAIN-2014-35"
    "CC-MAIN-2014-41"
    "CC-MAIN-2014-42"
    "CC-MAIN-2014-49"
    "CC-MAIN-2014-52"
    "CC-MAIN-2015-06"
    "CC-MAIN-2015-11"
    "CC-MAIN-2015-14"
    "CC-MAIN-2015-18"
    "CC-MAIN-2015-22"
    "CC-MAIN-2015-27"
    "CC-MAIN-2015-32"
    "CC-MAIN-2015-35"
    "CC-MAIN-2015-40"
    "CC-MAIN-2015-48"
    "CC-MAIN-2016-07"
    "CC-MAIN-2016-18"
    "CC-MAIN-2016-22"
    "CC-MAIN-2016-26"
    "CC-MAIN-2016-30"
    "CC-MAIN-2016-36"
    "CC-MAIN-2016-40"
    "CC-MAIN-2016-44"
    "CC-MAIN-2016-50"
    "CC-MAIN-2017-04"
    "CC-MAIN-2017-09"
    "CC-MAIN-2017-13"
    "CC-MAIN-2017-17"
    "CC-MAIN-2017-22"
    "CC-MAIN-2017-26"
    "CC-MAIN-2017-30"
    "CC-MAIN-2017-34"
    "CC-MAIN-2017-39"
    "CC-MAIN-2017-43"
    "CC-MAIN-2017-47"
    "CC-MAIN-2017-51"
    "CC-MAIN-2018-05"
    "CC-MAIN-2018-09"
    "CC-MAIN-2018-13"
    "CC-MAIN-2018-17"
    "CC-MAIN-2018-22"
    "CC-MAIN-2018-26"
    "CC-MAIN-2018-30"
    "CC-MAIN-2018-34"
    "CC-MAIN-2018-39"
    "CC-MAIN-2018-43"
    "CC-MAIN-2018-47"
    "CC-MAIN-2018-51"
    "CC-MAIN-2019-04"
    "CC-MAIN-2019-09"
    "CC-MAIN-2019-13"
    "CC-MAIN-2019-18"
    "CC-MAIN-2019-22"
    "CC-MAIN-2019-26"
    "CC-MAIN-2019-30"
    "CC-MAIN-2019-35"
    "CC-MAIN-2019-39"
    "CC-MAIN-2019-43"
    "CC-MAIN-2019-47"
    "CC-MAIN-2019-51"
    "CC-MAIN-2020-05"
    "CC-MAIN-2020-10"
    "CC-MAIN-2020-16"
    "CC-MAIN-2020-24"
    "CC-MAIN-2020-29"
    "CC-MAIN-2020-34"
    "CC-MAIN-2020-40"
    "CC-MAIN-2020-45"
    "CC-MAIN-2020-50"
    "CC-MAIN-2021-04"
    "CC-MAIN-2021-10"
    "CC-MAIN-2021-17"
    "CC-MAIN-2021-21"
    "CC-MAIN-2021-25"
    "CC-MAIN-2021-31"
    "CC-MAIN-2021-39"
    "CC-MAIN-2021-43"
    "CC-MAIN-2021-49"
    "CC-MAIN-2022-05"
    "CC-MAIN-2022-21"
    "CC-MAIN-2022-27"
    "CC-MAIN-2022-33"
    "CC-MAIN-2022-40"
    "CC-MAIN-2022-49"
    "CC-MAIN-2023-06"
    "CC-MAIN-2023-14"
    "CC-MAIN-2023-23"
    "CC-MAIN-2023-40"
    "CC-MAIN-2023-50"
    "CC-MAIN-2024-10"
    "CC-MAIN-2024-18"
    "CC-MAIN-2024-22"
    "CC-MAIN-2024-26"
    "CC-MAIN-2024-30"
)

# State file to track processed snapshots
STATE_FILE="txt360_processed.txt"

# Parse command line arguments
TEST_MODE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            TEST_MODE=true
            shift
            ;;
        *)
            echo "Unknown option $1"
            exit 1
            ;;
    esac
done

# Function to check if a snapshot has been processed
is_processed() {
    local snapshot="$1"
    if [[ -f "$STATE_FILE" ]]; then
        grep -q "^$snapshot$" "$STATE_FILE"
    else
        return 1
    fi
}

# Function to mark a snapshot as processed
mark_processed() {
    local snapshot="$1"
    echo "$snapshot" >> "$STATE_FILE"
}

# Function to submit a SLURM job for a snapshot
submit_job() {
    local snapshot="$1"
    local job_name="txt360_${snapshot}"
    
    sbatch --account=p32491 \
           --partition=normal \
           --nodes=1 \
           --ntasks-per-node=1 \
           --cpus-per-task=4 \
           --mem=32G \
           --time=48:00:00 \
           --job-name="$job_name" \
           --mail-user=nicholas.hagar@northwestern.edu \
           --mail-type=ALL \
           --wrap="module purge all; module load jq; uv run txt360_pipeline.py $snapshot --is_remote && echo $snapshot >> $STATE_FILE"
}

# Main processing loop
echo "Starting txt360 pipeline..."
echo "Test mode: $TEST_MODE"

processed_count=0
for snapshot in "${snapshots[@]}"; do
    if is_processed "$snapshot"; then
        echo "Skipping $snapshot (already processed)"
        continue
    fi
    
    echo "Submitting job for $snapshot"
    submit_job "$snapshot"
    processed_count=$((processed_count + 1))
    
    # In test mode, only submit one job
    if [[ "$TEST_MODE" == true ]]; then
        echo "Test mode: submitted 1 job, exiting"
        break
    fi
done

if [[ $processed_count -eq 0 ]]; then
    echo "All snapshots have been processed!"
else
    echo "Submitted $processed_count SLURM jobs"
fi