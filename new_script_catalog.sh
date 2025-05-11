#!/bin/bash
set -euo pipefail

# --- Configuration ---
# You can make these command-line arguments for the wrapper script too
DATASET="projecte-aina/catalog"
VARIANT="default"
CONCURRENCY=1 # Max parallel array tasks Slurm should run
CACHE_DIR_BASE=/scratch/nrh146/cache-catalog # Base dir for cache

# Derived names
CLEAN_DS_NAME=$(echo "$DATASET" | tr '/' '_' | tr '-' '_' | tr '.' '_')
VARIANT_NAME="$VARIANT"
STATUS_DIR="data/status/${CLEAN_DS_NAME}_${VARIANT_NAME}"
LOG_DIR="logs"

# Ensure log directory exists
mkdir -p "$LOG_DIR"
mkdir -p "$STATUS_DIR" # Ensure status dir exists
mkdir -p "$CACHE_DIR_BASE" # Ensure base cache dir exists

echo "======================================================"
echo "Starting Pipeline Submission for:"
echo "  Dataset: $DATASET"
echo "  Variant: $VARIANT"
echo "======================================================"

# --- Step 3: Identify Unprocessed Batches ---
# Directory containing batch files
batch_dir="data/${CLEAN_DS_NAME}_${VARIANT}"
# Collect indices of unprocessed batches
nums=()
for f in "$batch_dir"/download_urls_batch_*.txt; do
  [[ -f "$f" ]] || continue
  if [[ $(basename "$f") =~ download_urls_batch_([0-9]+)\.txt ]]; then
    nums+=("${BASH_REMATCH[1]}")
  fi
done
# Exit if nothing to process
if [ ${#nums[@]} -eq 0 ]; then
  echo "No unprocessed batch files found. Nothing to submit."
  exit 0
fi
# Create comma-separated list for sbatch array
array_spec=$(printf "%s," "${nums[@]}")
array_spec=${array_spec%,}

echo
echo "Setup successful. Found ${#nums[@]} unprocessed batches: ${array_spec}"

# --- Step 3: Submit Slurm Array Job ---
echo "Submitting Slurm array job (${array_spec} with max ${CONCURRENCY} concurrent tasks)..."

# Define the script/command that your array tasks will run
# Assuming your main processing script is pipeline.py and accepts necessary args
PROCESSING_SCRIPT="derived_dataset_pipeline_lfs.py" # Your main processing python script

sbatch_output=$(sbatch <<EOF
#!/bin/bash
#SBATCH --account=p32491  ## YOUR ACCOUNT pXXXX or bXXXX
#SBATCH --partition=normal  ### PARTITION (buyin, short, normal, etc)
#SBATCH --job-name="proc_${CLEAN_DS_NAME}_${VARIANT_NAME}"
#SBATCH --array=${array_spec}%${CONCURRENCY}
#SBATCH --output="${LOG_DIR}/slurm-%A_%a.out"  # %A=jobid, %a=taskid
#SBATCH --error="${LOG_DIR}/slurm-%A_%a.err"
#SBATCH --nodes=1                # Ensure tasks run on single nodes unless needed otherwise
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=48:00:00 ## how long does this need to run (remember different partitions have restrictions on this parameter)
#SBATCH --mail-user=nicholas.hagar@northwestern.edu
#SBATCH --mail-type=ALL

echo "Starting Slurm task \$SLURM_ARRAY_TASK_ID for job \$SLURM_ARRAY_JOB_ID"
echo "Node: \$(hostname)"
echo "Requesting 1 task with \$SLURM_CPUS_PER_TASK CPUs."


module purge all

batchfile="data/${CLEAN_DS_NAME}_${VARIANT}/download_urls_batch_\$SLURM_ARRAY_TASK_ID.txt"

if [ ! -f "\$batchfile" ]; then
  echo "Batch file \$batchfile not found; already processed. Exiting."
  exit 0
fi
echo "Batch file found: \$batchfile"

# Create a temporary file to store paths for xargs
temp_include_file=$(mktemp)

# Process the batch file and store paths in the temporary file
while IFS= read -r path || [[ -n "\$path" ]]; do
  # Skip empty lines or lines that look like comments
  [[ -z "\$path" || "\$path" =~ ^# ]] && continue
  # Write the path to the temp file
  printf "%s\0" "\$path" >> "\$temp_include_file"
done < "\$batchfile"

# Check if we have any paths
if [ ! -s "\$temp_include_file" ]; then
  echo "No valid file paths found in \$batchfile"
  rm "\$temp_include_file"
  exit 1
fi

# --- Step 4: Download files using wget (single-threaded) ---
echo "Downloading files using wget (single-threaded)..."

# Create directory for downloads
DOWNLOAD_DIR="$CACHE_DIR_BASE/repo"
mkdir -p "\$DOWNLOAD_DIR"

# Change to the download directory
cd "\$DOWNLOAD_DIR" || {
  echo "Error: Failed to change directory to \$DOWNLOAD_DIR."
  rm "\$temp_include_file" # Clean up temp file
  exit 1
}

# Use a simple loop to download files one at a time
while IFS= read -r -d $'\0' url; do
  echo "Downloading: \$url"
  wget --quiet -nc -c "\$url" || {
    echo "Error: Failed to download \$url"
    continue
  }
done < "\$temp_include_file"

echo "Download completed successfully."

# Clean up the temporary file
rm "\$temp_include_file"

# cd back to the original directory
cd - || exit 1

# --- Step 5: Run Python processing script ---
uv run python "$PROCESSING_SCRIPT" \
    --dataset "$DATASET" \
    --variant "$VARIANT" \
    --cache-dir "$CACHE_DIR_BASE"

EXIT_CODE=\$?
echo "Processing script finished with exit code: \$EXIT_CODE"


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