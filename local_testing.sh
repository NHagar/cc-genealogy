#!/bin/bash
set -euo pipefail


# --- Configuration ---
# You can make these command-line arguments for the wrapper script too
DATASET="zyphra/zyda-2"
VARIANT="dclm_crossdeduped"
BATCH_SIZE=20000000000
CONCURRENCY=1 # Max parallel array tasks Slurm should run

# Derived names
CLEAN_DS_NAME=$(echo "$DATASET" | tr '/' '_' | tr '-' '_' | tr '.' '_')
VARIANT_NAME="$VARIANT"
STATUS_DIR="data/status/${CLEAN_DS_NAME}_${VARIANT_NAME}"
LOG_DIR="logs"
CACHE_DIR_BASE=data/cache-zyda2 # Base dir for cache

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

# --- Step 2: Clone repo to cache dir (if needed) ---
# Check if the cache directory already exists
# Set the repo URL 
REPO_URL="https://huggingface.co/datasets/${DATASET}"

# Set the full cache directory path
CACHE_DIR="${CACHE_DIR_BASE}/repo"

# Check if the cache directory already exists
if [ -d "$CACHE_DIR" ]; then
  echo "Cache directory already exists at $CACHE_DIR. Skipping clone."
else
  echo "Cache directory not found. Cloning repository to $CACHE_DIR..."
  GIT_LFS_SKIP_SMUDGE=1 git clone "$REPO_URL" "$CACHE_DIR"
  
  if [ $? -ne 0 ]; then
    echo "Error: Failed to clone repository. Aborting."
    exit 1
  fi
  
  echo "Repository successfully cloned to cache directory."

fi


# --- Step 3: Extract Batch Count ---
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


batchfile="$(ls data/${CLEAN_DS_NAME}_${VARIANT}/download_urls_batch_*.txt 2>/dev/null | sort -V | head -n1 || true)"

if [ -z "$batchfile" ]; then
    echo "No batch file found. Exiting."
    exit 1
fi

echo "Batch file found: $batchfile"

# Create a temporary file to store paths for xargs
temp_include_file=$(mktemp)

# Process the batch file and store paths in the temporary file
while IFS= read -r path; do
  # Skip empty lines or lines that look like comments
  [[ -z "$path" || "$path" =~ ^# ]] && continue
  # Write the path to the temp file
  echo "$path" >> "$temp_include_file"
done < "$batchfile"

# Check if we have any paths
if [ ! -s "$temp_include_file" ]; then
  echo "No valid file paths found in $batchfile"
  rm "$temp_include_file"
  exit 1
fi


# --- Step 4: Run git lfs pull ---
cd ${CACHE_DIR} || {
  echo "Error: Failed to change directory to $CACHE_DIR."
  rm "$temp_include_file" # Clean up temp file
  exit 1
}

BATCH_PATH_LIMIT=1000

echo "Running git lfs pull with comma-separated paths (batch size: $BATCH_PATH_LIMIT)..."

# Process the files in batches using xargs
cat "$temp_include_file" | xargs -n $BATCH_PATH_LIMIT sh -c '
    # Count files properly using number of arguments
    file_count=$#
    echo "Pulling batch of ${file_count} files..."
    
    # Create a properly escaped comma-separated list
    paths=""
    for file in "$@"; do
        if [ -z "$paths" ]; then
            paths="$file"
        else
            paths="$paths,$file"
        fi
    done
    
    # Execute git lfs pull with the single --include flag and comma-separated list
    git lfs pull --include="$paths"
' _ || {
    echo "Error: git lfs pull with comma-separated paths failed."
    cd - > /dev/null
    rm "$temp_include_file"
    exit 1
}

echo "git lfs pull completed successfully."

# Clean up the temporary file
rm "$temp_include_file"

# cd back to the original directory
cd - || exit 1

# --- Step 5: Run Python processing script ---
echo "Running Python processing script..."
uv run derived_dataset_pipeline_lfs.py \
    --dataset "$DATASET" \
    --variant "$VARIANT" \
    --cache-dir "$CACHE_DIR_BASE" \