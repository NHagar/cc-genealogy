#!/bin/bash

# --- Configuration ---
# You can make these command-line arguments for the wrapper script too
DATASET="zyphra/zyda"
VARIANT="default"
BATCH_SIZE=100000000000
CONCURRENCY=8 # Max parallel array tasks Slurm should run

# Derived names
CLEAN_DS_NAME=$(echo "$DATASET" | tr '/' '_' | tr '-' '_' | tr '.' '_')
VARIANT_NAME="$VARIANT"
BATCH_FILE_DIR="data/batch_files/${CLEAN_DS_NAME}_${VARIANT_NAME}"
STATUS_DIR="data/status/${CLEAN_DS_NAME}_${VARIANT_NAME}"
LOG_DIR="logs"
CACHE_DIR_BASE=/scratch/nrh146/cache-zyda # Base dir for cache

# Ensure log directory exists
mkdir -p "$LOG_DIR"
mkdir -p "$STATUS_DIR" # Ensure status dir exists
mkdir -p "$CACHE_DIR_BASE" # Ensure base cache dir exists

echo "======================================================"
echo "Starting Pipeline Submission for:"
echo "  Dataset: $DATASET"
echo "  Variant: $VARIANT"
echo "======================================================"

# --- Step 1: Run Setup Script ---
echo
echo "Running setup script..."
setup_output=$(uv run setup_pipeline.py \
    --dataset "$DATASET" \
    --variant "$VARIANT" \
    --batch-size $BATCH_SIZE)

# Capture exit code of setup script
setup_exit_code=$?

# Print the full output of the setup script for debugging/logging
echo "--- Setup Script Output ---"
echo "$setup_output"
echo "-------------------------"

# Check if the setup script exited successfully
if [ $setup_exit_code -ne 0 ]; then
  echo "Error: Setup script failed with exit code $setup_exit_code. Aborting."
  exit 1
fi

# --- Step 2: Extract Batch Count ---
# Extract the last line of the output, which should be the number
num_batches=$(echo "$setup_output" | tail -n 1)

# Validate if it's a positive integer
if ! [[ "$num_batches" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: Could not extract a valid positive integer batch count from setup script."
  echo "Extracted value: '$num_batches'"
  # Check if it was 0, maybe that's valid?
  if [[ "$num_batches" == "0" ]]; then
       echo "Setup reported 0 batches. Nothing to submit."
       exit 0 # Successfully did nothing
  fi
  exit 1 # Exit on invalid non-zero value
fi

echo
echo "Setup successful. Found $num_batches batches."

# --- Step 3: Submit Slurm Array Job ---
echo "Submitting Slurm array job (1-$num_batches with max ${CONCURRENCY} concurrent tasks)..."

# Define the script/command that your array tasks will run
# Assuming your main processing script is pipeline.py and accepts necessary args
PROCESSING_SCRIPT="derived_dataset_pipeline_aria.py" # Your main processing python script

sbatch_output=$(sbatch <<EOF
#!/bin/bash
#SBATCH --account=p32491  ## YOUR ACCOUNT pXXXX or bXXXX
#SBATCH --partition=normal  ### PARTITION (buyin, short, normal, etc)
#SBATCH --ntasks-per-node=8 ## how many cpus or processors do you need on each computer
#SBATCH --job-name="proc_${CLEAN_DS_NAME}_${VARIANT_NAME}"
#SBATCH --array=1-${num_batches}%${CONCURRENCY}
#SBATCH --output="${LOG_DIR}/slurm-%A_%a.out"  # %A=jobid, %a=taskid
#SBATCH --error="${LOG_DIR}/slurm-%A_%a.err"
#SBATCH --nodes=1                # Ensure tasks run on single nodes unless needed otherwise
#SBATCH --mem-per-cpu=8G ## how much RAM do you need per node (this effects your FairShare score so be careful to not ask for more than you need))
#SBATCH --time=48:00:00 ## how long does this need to run (remember different partitions have restrictions on this parameter)
#SBATCH --mail-user=nicholas.hagar@northwestern.edu
#SBATCH --mail-type=ALL

echo "Starting Slurm task \$SLURM_ARRAY_TASK_ID for job \$SLURM_ARRAY_JOB_ID"
echo "Dataset: $DATASET"
echo "Variant: $VARIANT"

module purge all
conda activate aria2-env

uv run $PROCESSING_SCRIPT \\
    --dataset "$DATASET" \\
    --variant "$VARIANT" \\
    --num-proc \$SLURM_CPUS_PER_TASK \\
    --cache-dir "${CACHE_DIR_BASE}" \\

echo "Finished Slurm task \$SLURM_ARRAY_TASK_ID"
EOF
)

sbatch_exit_code=$?

if [ $sbatch_exit_code -ne 0 ]; then
  echo "Error: sbatch command failed."
  exit 1
fi

echo "$sbatch_output" # Print the "Submitted batch job XXX" message
echo "Slurm array job submitted successfully."
echo "======================================================"

exit 0