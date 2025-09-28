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
SNAPSHOTS_FILE="snapshots_list.txt"

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

# Function to create snapshots list file for unprocessed snapshots
create_snapshots_list() {
    > "$SNAPSHOTS_FILE"
    local unprocessed_count=0
    
    for snapshot in "${snapshots[@]}"; do
        if ! is_processed "$snapshot"; then
            echo "$snapshot" >> "$SNAPSHOTS_FILE"
            unprocessed_count=$((unprocessed_count + 1))
        fi
    done
    
    echo $unprocessed_count
}

# Function to submit SLURM job array
submit_job_array() {
    local num_jobs="$1"
    
    if [[ "$TEST_MODE" == true ]]; then
        num_jobs=1
    fi
    
    if [[ $num_jobs -eq 0 ]]; then
        echo "No jobs to submit - all snapshots already processed"
        return
    fi
    
    local array_spec="1-${num_jobs}"
    if [[ "$TEST_MODE" == true ]]; then
        array_spec="1"
    fi
    
    sbatch --account=p32491 \
           --partition=normal \
           --nodes=1 \
           --ntasks-per-node=1 \
           --cpus-per-task=4 \
           --mem=100G \
           --time=48:00:00 \
           --job-name="txt360_array" \
           --mail-user=nicholas.hagar@northwestern.edu \
           --mail-type=ALL \
           --array="$array_spec"%1 \
           --wrap="
               SNAPSHOT=\$(sed -n \"\${SLURM_ARRAY_TASK_ID}p\" $SNAPSHOTS_FILE)
               echo \"Processing snapshot: \$SNAPSHOT\"
               module purge all
               module load jq
               uv run txt360_pipeline.py \$SNAPSHOT --is_remote
               if [[ \$? -eq 0 ]]; then
                   echo \$SNAPSHOT >> $STATE_FILE
                   echo \"Successfully processed: \$SNAPSHOT\"
               else
                   echo \"Failed to process: \$SNAPSHOT\"
                   exit 1
               fi
           "
}

# Main processing
echo "Starting txt360 pipeline with job array..."
echo "Test mode: $TEST_MODE"

# Create list of unprocessed snapshots
unprocessed_count=$(create_snapshots_list)

if [[ $unprocessed_count -eq 0 ]]; then
    echo "All snapshots have been processed!"
    rm -f "$SNAPSHOTS_FILE"
    exit 0
fi

echo "Found $unprocessed_count unprocessed snapshots"

if [[ "$TEST_MODE" == true ]]; then
    echo "Test mode: submitting job array for 1 snapshot"
else
    echo "Submitting job array for $unprocessed_count snapshots (max 1 concurrent job)"
fi

submit_job_array "$unprocessed_count"

echo "Job array submitted. Jobs will process sequentially with max concurrency of 1."
echo "Snapshot list saved to: $SNAPSHOTS_FILE"
echo "Progress tracking file: $STATE_FILE"