#!/bin/bash
set -euo pipefail

# --- Configuration ---
DATASET="allenai/dolma"
VARIANT="v1_5"
# Max parallel wget processes to run on this (orchestrator) node for each batch download
PARALLEL_WGET_JOBS_PER_BATCH=4 # Adjust based on login node policy and network capacity
CACHE_DIR_BASE="/scratch/nrh146/cache-dolma-v1_5"
PROCESSING_SCRIPT="derived_dataset_pipeline_dolma.py" # Name of the new Python script

# Slurm Configuration
SLURM_ACCOUNT="p32491"
SLURM_PARTITION="normal"
SLURM_CPUS_PER_TASK=4
SLURM_MEM="32G"
SLURM_TIME="48:00:00" # Time for the Python processing job
MAIL_USER="nicholas.hagar@northwestern.edu"
MAIL_TYPE="FAIL,END"

# Derived names
CLEAN_DS_NAME=$(echo "$DATASET" | tr '/' '_' | tr '-' '_' | tr '.' '_')
VARIANT_NAME="$VARIANT"
BATCH_URL_FILES_DIR="data/${CLEAN_DS_NAME}_${VARIANT}"
# All downloaded files for the *current* batch go here. Python script will clear it.
DOWNLOAD_PROCESSING_DIR="$CACHE_DIR_BASE/repo_active_batch"
ORCHESTRATOR_STATUS_DIR="data/orchestrator_status_simplified/${CLEAN_DS_NAME}_${VARIANT_NAME}"
LOG_DIR="logs"
SLURM_JOB_LOG_DIR="${LOG_DIR}/slurm_simplified_jobs"

# --- Ensure directories exist ---
mkdir -p "$LOG_DIR"
mkdir -p "$SLURM_JOB_LOG_DIR"
mkdir -p "$ORCHESTRATOR_STATUS_DIR"
mkdir -p "$CACHE_DIR_BASE"
mkdir -p "$DOWNLOAD_PROCESSING_DIR" # Critical for the Python script

# --- Log File for Orchestrator ---
ORCHESTRATOR_LOG_FILE="${LOG_DIR}/orchestrator_simplified_${CLEAN_DS_NAME}_${VARIANT_NAME}_$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "${ORCHESTRATOR_LOG_FILE}") 2>&1

echo "======================================================================"
echo "Simplified Orchestrator Started: $(date)"
echo "  Dataset: $DATASET, Variant: $VARIANT"
echo "  Parallel Wget Jobs (on this node per batch): $PARALLEL_WGET_JOBS_PER_BATCH"
echo "  Orchestrator Log: $ORCHESTRATOR_LOG_FILE"
echo "  Download/Processing Directory: $DOWNLOAD_PROCESSING_DIR"
echo "======================================================================"

# --- Helper Functions ---
log_event() {
    local context="$1"
    local event_type="$2"
    local message="${3:-}"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [$context] [$event_type] $message"
}

download_current_batch_files() {
    local batch_url_file_path="$1"
    local batch_display_name # For logging, extract from path
    batch_display_name=$(basename "$batch_url_file_path" .txt)


    echo "[Orchestrator - $batch_display_name] Starting parallel download to $DOWNLOAD_PROCESSING_DIR..."
    log_event "$batch_display_name" "Download_Start"

    if [ ! -f "$batch_url_file_path" ]; then
        echo "[Orchestrator - $batch_display_name] ERROR: URL file $batch_url_file_path not found."
        log_event "$batch_display_name" "Download_Error_NoUrlFile"
        return 1
    fi

    # Ensure the download/processing directory is empty before downloading a new batch
    # The Python script should have cleaned it from the previous run.
    # As a safeguard, we can check and warn or clean here.
    if [ "$(ls -A "$DOWNLOAD_PROCESSING_DIR")" ]; then
       echo "[Orchestrator - $batch_display_name] WARNING: Download directory $DOWNLOAD_PROCESSING_DIR is not empty before new download. Previous Python script might not have cleaned up, or this is an unexpected state. Clearing it now."
       log_event "$batch_display_name" "Download_Warning_DirNotEmpty_Cleaning"
       rm -rf "${DOWNLOAD_PROCESSING_DIR:?}"/* # Protect against empty var
    fi


    local temp_url_list_file
    temp_url_list_file=$(mktemp "/tmp/dolma_wget_urls_${batch_display_name}_XXXXXX.txt")
    trap 'rm -f "$temp_url_list_file"' RETURN

    local url_count=0
    while IFS= read -r path || [[ -n "$path" ]]; do
        [[ -z "$path" || "$path" =~ ^# ]] && continue
        printf "%s\0" "$path" >> "$temp_url_list_file"
        url_count=$((url_count + 1))
    done < "$batch_url_file_path"

    if [ "$url_count" -eq 0 ]; then
        echo "[Orchestrator - $batch_display_name] No valid URLs found in $batch_url_file_path."
        log_event "$batch_display_name" "Download_Error_NoValidUrls"
        return 1
    fi
    echo "[Orchestrator - $batch_display_name] Found $url_count URLs to download."

    if xargs -0 -r -n 1 -P "$PARALLEL_WGET_JOBS_PER_BATCH" -I {} \
        wget -P "$DOWNLOAD_PROCESSING_DIR" \
            --no-verbose -nc -c --tries=3 \
            --dns-timeout=30 --connect-timeout=45 --timeout=90 --read-timeout=180 \
            --user-agent="DolmaSimplifiedOrchestratorWget/1.0 (Contact/$MAIL_USER)" \
            "{}" < "$temp_url_list_file"; then
        echo "[Orchestrator - $batch_display_name] Parallel download process finished successfully."
        log_event "$batch_display_name" "Download_Success"
        return 0
    else
        local xargs_exit_code=$?
        echo "[Orchestrator - $batch_display_name] ERROR: Parallel download process (xargs) exited with code $xargs_exit_code. Some downloads likely failed. Halting for this batch."
        log_event "$batch_display_name" "Download_Error_XargsExitCode${xargs_exit_code}"
        return 1 # Critical failure if any wget fails
    fi
}

submit_and_wait_for_processing_job() {
    local current_batch_id_for_output="$1" # e.g., "1", "2" for naming outputs

    echo "[Orchestrator - BatchOutputID $current_batch_id_for_output] Submitting Slurm job for Python processing..."
    log_event "BatchOutputID $current_batch_id_for_output" "SlurmSubmit_Start"

    local sbatch_cmd_output
    # Use sbatch --wait to make the orchestrator script pause until the job completes
    # This simplifies waiting logic significantly.
    # Ensure your Slurm version supports --wait and it behaves as expected (waits for job completion, not just submission).
    # If --wait is problematic, revert to polling squeue.
    sbatch_cmd_output=$(sbatch --wait --parsable <<EOF
#!/bin/bash
#SBATCH --account=${SLURM_ACCOUNT}
#SBATCH --partition=${SLURM_PARTITION}
#SBATCH --job-name="py_simp_${CLEAN_DS_NAME}_${VARIANT_NAME}_b${current_batch_id_for_output}"
#SBATCH --output="${SLURM_JOB_LOG_DIR}/slurm-%j_batch_output_id-${current_batch_id_for_output}.out"
#SBATCH --error="${SLURM_JOB_LOG_DIR}/slurm-%j_batch_output_id-${current_batch_id_for_output}.err"
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=${SLURM_CPUS_PER_TASK}
#SBATCH --mem=${SLURM_MEM}
#SBATCH --time=${SLURM_TIME}
#SBATCH --mail-user=${MAIL_USER}
#SBATCH --mail-type=${MAIL_TYPE}

echo "Slurm Job ID: \$SLURM_JOB_ID, Python Processing for Output Batch ID: $current_batch_id_for_output"
echo "Node: \$(hostname) at \$(date)"
echo "Dataset: $DATASET, Variant: $VARIANT, Cache Base: $CACHE_DIR_BASE"
echo "Python script will process files from: $DOWNLOAD_PROCESSING_DIR"

module purge all
# Add module loads if needed for 'uv' or Python on compute nodes

echo "Executing: uv run python $PROCESSING_SCRIPT --dataset \"$DATASET\" --variant \"$VARIANT\" --cache-dir \"$CACHE_DIR_BASE\" --current-output-batch-id \"$current_batch_id_for_output\""
uv run "$PROCESSING_SCRIPT" \\
    --dataset "$DATASET" \\
    --variant "$VARIANT" \\
    --cache-dir "$CACHE_DIR_BASE" \\
    --current-output-batch-id "$current_batch_id_for_output" # For naming outputs

PYTHON_EXIT_CODE=\$?
echo "Python processing for Output Batch ID $current_batch_id_for_output finished with Exit Code: \$PYTHON_EXIT_CODE at \$(date)"

if [ \$PYTHON_EXIT_CODE -eq 0 ]; then
    touch "${ORCHESTRATOR_STATUS_DIR}/batch_output_id_${current_batch_id_for_output}.job_\$SLURM_JOB_ID.processed_ok"
else
    touch "${ORCHESTRATOR_STATUS_DIR}/batch_output_id_${current_batch_id_for_output}.job_\$SLURM_JOB_ID.processed_fail"
fi
exit \$PYTHON_EXIT_CODE
EOF
)
    local sbatch_submission_exit_code=$? # Exit code of the sbatch command itself
    local slurm_job_id
    
    if [[ "$sbatch_cmd_output" =~ ^[0-9]+$ ]]; then
        slurm_job_id="$sbatch_cmd_output"
        echo "[Orchestrator - BatchOutputID $current_batch_id_for_output] Slurm job $slurm_job_id submitted."
        log_event "BatchOutputID $current_batch_id_for_output" "SlurmSubmit_Success_JobID${slurm_job_id}"
    else
        echo "[Orchestrator - BatchOutputID $current_batch_id_for_output] ERROR: Failed to parse Slurm Job ID from sbatch output: '$sbatch_cmd_output'"
        log_event "BatchOutputID $current_batch_id_for_output" "SlurmSubmit_Error_NoJobID"
        return 1 # sbatch submission failed to give a job ID
    fi

    # Since we used sbatch --wait, the script pauses here until the job finishes.
    # After sbatch --wait returns, we need to check the actual exit status of the Slurm job.
    # sacct can be used for this.
    # Wait a few seconds for sacct to update
    sleep 10 
    local job_final_status
    job_final_status=$(sacct -j "$slurm_job_id" --format=State --noheader | head -n 1 | awk '{print $1}')
    local job_final_exit_code_pair
    job_final_exit_code_pair=$(sacct -j "$slurm_job_id" --format=ExitCode --noheader | head -n 1 | awk '{print $1}')
    local job_final_exit_code=${job_final_exit_code_pair%%:*}


    echo "[Orchestrator - BatchOutputID $current_batch_id_for_output] Slurm job $slurm_job_id finished. Status: $job_final_status, ExitCode: $job_final_exit_code_pair (parsed as $job_final_exit_code)."
    log_event "BatchOutputID $current_batch_id_for_output" "SlurmJob_Completed_JobID${slurm_job_id}_Status${job_final_status}_Exit${job_final_exit_code}"

    if [[ "$job_final_status" == "COMPLETED" ]] && [[ "$job_final_exit_code" -eq 0 ]]; then
        echo "[Orchestrator - BatchOutputID $current_batch_id_for_output] Python processing SUCCEEDED."
        return 0
    else
        echo "[Orchestrator - BatchOutputID $current_batch_id_for_output] ERROR: Python processing FAILED. Slurm Status: $job_final_status, Exit Code: $job_final_exit_code."
        # The .processed_fail file should have been created by the Slurm script itself.
        return 1
    fi
}


# --- Main Orchestration Loop ---
all_batch_url_files=()
for f in "${BATCH_URL_FILES_DIR}"/download_urls_batch_*.txt; do
    [[ -f "$f" ]] && all_batch_url_files+=("$f")
done

if [ ${#all_batch_url_files[@]} -eq 0 ]; then
    echo "[Orchestrator] No batch URL files found in $BATCH_URL_FILES_DIR. Exiting."
    log_event "Orchestrator" "MainLoop_Error_NoBatchFiles"
    exit 0
fi

# Sort batch files to process them in order (e.g., batch_1, batch_2, ...)
IFS=$'\n' sorted_batch_url_files=($(sort -V <<<"${all_batch_url_files[*]}"))
unset IFS

TOTAL_BATCHES=${#sorted_batch_url_files[@]}
echo "[Orchestrator] Found $TOTAL_BATCHES total batch URL files to process."
log_event "Orchestrator" "MainLoop_Start_TotalBatches${TOTAL_BATCHES}"

processed_count=0
failed_count=0

for batch_url_file_path in "${sorted_batch_url_files[@]}"; do
    current_batch_filename=$(basename "$batch_url_file_path")
    # Extract batch number from filename like download_urls_batch_1.txt for output ID
    if [[ "$current_batch_filename" =~ download_urls_batch_([0-9]+)\.txt ]]; then
        current_output_id="${BASH_REMATCH[1]}"
    else
        echo "[Orchestrator] Could not parse batch number from $current_batch_filename. Skipping."
        log_event "$current_batch_filename" "Error_ParseBatchNum"
        failed_count=$((failed_count + 1))
        continue
    fi

    echo "----------------------------------------------------------------------"
    log_event "BatchOutputID $current_output_id" "Processing_Start_File${current_batch_filename}"
    echo "[Orchestrator] Processing $current_batch_filename (Output ID: $current_output_id)..."

    # Status file to check if this batch's output ID was already successfully processed
    # Note: This status file is for the *output batch ID*, not the Slurm job ID.
    # The Slurm job itself creates a more specific status file with its job ID.
    processed_ok_marker="${ORCHESTRATOR_STATUS_DIR}/batch_output_id_${current_output_id}.processed_ok_overall"
    processing_failed_marker="${ORCHESTRATOR_STATUS_DIR}/batch_output_id_${current_output_id}.failed_overall"

    if [ -f "$processed_ok_marker" ]; then
        echo "[Orchestrator - BatchOutputID $current_output_id] Already marked as successfully processed overall. Skipping."
        log_event "BatchOutputID $current_output_id" "AlreadyProcessedOverall_Skipping"
        processed_count=$((processed_count + 1))
        continue
    elif [ -f "$processing_failed_marker" ]; then
         echo "[Orchestrator - BatchOutputID $current_output_id] Marked as failed overall. Skipping."
         log_event "BatchOutputID $current_output_id" "AlreadyFailedOverall_Skipping"
         failed_count=$((failed_count + 1))
         continue
    fi

    # Step 1: Download files for the current batch
    if ! download_current_batch_files "$batch_url_file_path"; then
        echo "[Orchestrator - BatchOutputID $current_output_id] Download failed. Skipping Python processing for this batch."
        log_event "BatchOutputID $current_output_id" "DownloadFailed_SkippingProcessing"
        failed_count=$((failed_count + 1))
        touch "$processing_failed_marker" # Mark as failed overall due to download
        continue # Move to the next batch file
    fi

    # Step 2: Submit Slurm job for Python processing and wait for it
    if submit_and_wait_for_processing_job "$current_output_id"; then
        echo "[Orchestrator - BatchOutputID $current_output_id] Python processing completed successfully."
        log_event "BatchOutputID $current_output_id" "PythonProcessing_Success"
        processed_count=$((processed_count + 1))
        touch "$processed_ok_marker" # Mark as successfully processed overall
    else
        echo "[Orchestrator - BatchOutputID $current_output_id] Python processing failed."
        log_event "BatchOutputID $current_output_id" "PythonProcessing_Failed"
        failed_count=$((failed_count + 1))
        touch "$processing_failed_marker" # Mark as failed overall due to processing
        # Decide if you want to stop the entire orchestrator on the first Python processing failure
        # echo "[Orchestrator] CRITICAL: Python processing failed for Output ID $current_output_id. Halting orchestrator."
        # exit 1
    fi
done

echo "======================================================================"
echo "Simplified Orchestration Complete: $(date)"
echo "  Total Batch URL Files Found: $TOTAL_BATCHES"
echo "  Successfully Processed Batches: $processed_count"
echo "  Failed/Skipped Batches: $failed_count"
echo "======================================================================"
log_event "Orchestrator" "RunComplete_Success${processed_count}_Fail${failed_count}"

exit 0
