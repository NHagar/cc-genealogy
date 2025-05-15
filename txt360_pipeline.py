import os
import shutil
import subprocess

import duckdb
import pandas as pd
import tldextract
from huggingface_hub import HfApi, hf_hub_download
from tqdm import tqdm  # Optional: for progress bars

# --- Configuration ---
REPO_ID = "LLM360/TxT360"  # REQUIRED: Replace with your dataset ID
REPO_TYPE = "dataset"  # Or "model", "space"
DIRECTORY_IN_REPO = "data/common-crawl"

LOCAL_DOWNLOAD_BASE_DIR = "/scratch/nrh146/hf_batches_data_fail_fast"  # Base directory
STATE_FILE = os.path.join(LOCAL_DOWNLOAD_BASE_DIR, "processed_files.txt")
TARGET_BATCH_SIZE_GB = 500
TARGET_BATCH_SIZE_BYTES = TARGET_BATCH_SIZE_GB * (1024**3)

# --- Helper Functions ---
con = duckdb.connect(database=":memory:", read_only=False)


def load_processed_files(state_file_path):
    """Loads the set of processed file paths from the state file. Fails if errors."""
    if not os.path.exists(state_file_path):
        return set()
    # If file exists but is unreadable, this will now raise an IOError/OSError
    with open(state_file_path, "r") as f:
        return set(line.strip() for line in f if line.strip())


def mark_file_as_processed(state_file_path, repo_file_path):
    """Marks a file as processed. Fails if errors."""
    os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
    # If file cannot be written, this will now raise an IOError/OSError
    with open(state_file_path, "a") as f:
        f.write(repo_file_path + "\n")


def extract_domain(url: str) -> str:
    if url is None:
        return None
    try:
        extracted = tldextract.extract(url)
        # Return the full domain information as a formatted string
        return f"{extracted.domain}.{extracted.suffix}"
    except Exception:
        return None  # Handle potential errors in tldextract


def custom_process_file(fpath, selector):
    command = f"zcat {fpath} | jq -r '.{selector}'"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Failed to process file {fpath}: {result.stderr}")
        raise Exception(f"Failed to process file {fpath}: {result.stderr}")
    extracted_urls = result.stdout.splitlines()
    domains = [extract_domain(url) for url in extracted_urls]

    df = pd.DataFrame({"url": extracted_urls, "domain": domains})
    df = df.dropna()
    df.to_parquet(
        fpath.replace("jsonl.gz", "parquet"),
        index=False,
        compression="zstd",
        engine="pyarrow",
    )
    return True


def main():
    """
    Main function to download, process, and manage dataset files in batches.
    This version will fail fast on errors in critical operations.
    """
    print("--- Hugging Face Batch Downloader & Processor (Fail Fast Mode) ---")
    print(f"Dataset ID: {REPO_ID}")
    print(f"Directory in Repo: '{DIRECTORY_IN_REPO if DIRECTORY_IN_REPO else 'Root'}'")
    print(f"Target batch size: {TARGET_BATCH_SIZE_GB} GB")
    print(f"Local base directory: {LOCAL_DOWNLOAD_BASE_DIR}")
    print(f"State file: {STATE_FILE}")
    print("-" * 50)

    if REPO_ID == "your-username/your-dataset-name":
        # This is a configuration error, so a print and return is reasonable.
        print(
            "ERROR: Please update REPO_ID in the script with your Hugging Face dataset ID."
        )
        return

    os.makedirs(LOCAL_DOWNLOAD_BASE_DIR, exist_ok=True)

    # If load_processed_files fails (e.g., permission denied, corrupted file), script will exit.
    processed_repo_file_paths = load_processed_files(STATE_FILE)
    print(
        f"Loaded {len(processed_repo_file_paths)} already processed file paths from state."
    )

    api = HfApi()
    print(f"Fetching file list from repo: {REPO_ID}...")
    # If api.list_repo_files fails (network issue, auth error), script will exit.
    all_repo_files_details = api.list_repo_files(repo_id=REPO_ID, repo_type=REPO_TYPE)

    if DIRECTORY_IN_REPO:
        dir_prefix = (
            DIRECTORY_IN_REPO
            if DIRECTORY_IN_REPO.endswith("/")
            else f"{DIRECTORY_IN_REPO}/"
        )
        if DIRECTORY_IN_REPO == "/":
            dir_prefix = ""
        all_repo_files = [
            f
            for f in all_repo_files_details
            if f.startswith(dir_prefix) and f != dir_prefix
        ]
        if not all_repo_files and dir_prefix:
            if DIRECTORY_IN_REPO in all_repo_files_details:
                all_repo_files = [DIRECTORY_IN_REPO]
    else:
        all_repo_files = all_repo_files_details
    print(f"Found {len(all_repo_files)} files in specified repo path.")

    files_to_process = sorted(
        [f for f in all_repo_files if f not in processed_repo_file_paths]
    )

    if not files_to_process:
        print(
            "No new files to process. All files in the specified directory are already marked as processed."
        )
        return

    print(f"{len(files_to_process)} files remaining to be processed.")

    current_batch_files_info = []
    current_batch_size_bytes = 0
    batch_number = 1

    for i, repo_file_path in enumerate(tqdm(files_to_process, desc="Overall Progress")):
        current_batch_download_dir = os.path.join(
            LOCAL_DOWNLOAD_BASE_DIR, f"batch_{batch_number}"
        )
        os.makedirs(current_batch_download_dir, exist_ok=True)

        local_target_path = os.path.join(current_batch_download_dir, repo_file_path)
        print(f"\nAttempting to download: {repo_file_path} to {local_target_path}")

        # If hf_hub_download fails (network, disk space, permissions), script will exit.
        downloaded_file_actual_path = hf_hub_download(
            repo_id=REPO_ID,
            filename=repo_file_path,
            repo_type=REPO_TYPE,
            local_dir=current_batch_download_dir,
            resume_download=True,  # hf_hub_download has its own resilience for the download itself
        )

        # If os.path.getsize fails (e.g., file just deleted by another process), script will exit.
        file_size_bytes = os.path.getsize(downloaded_file_actual_path)
        print(f"Downloaded {repo_file_path} ({file_size_bytes / (1024 * 1024):.2f} MB)")

        current_batch_files_info.append(
            {
                "repo_path": repo_file_path,
                "local_path": downloaded_file_actual_path,
                "size": file_size_bytes,
            }
        )
        current_batch_size_bytes += file_size_bytes

        is_last_file = i == len(files_to_process) - 1
        if current_batch_size_bytes >= TARGET_BATCH_SIZE_BYTES or (
            is_last_file and current_batch_files_info
        ):
            print(f"\n--- Processing Batch {batch_number} ---")
            print(f"Target size: {TARGET_BATCH_SIZE_BYTES / (1024**3):.2f} GB")
            print(f"Actual batch size: {current_batch_size_bytes / (1024**3):.2f} GB")
            print(f"Files in batch: {len(current_batch_files_info)}")

            for file_info in tqdm(
                current_batch_files_info, desc=f"Files in Batch {batch_number}"
            ):
                # If custom_process_file raises an exception, script will exit.
                custom_process_file(file_info["local_path"], "url")
                # If mark_file_as_processed fails, script will exit.
                mark_file_as_processed(STATE_FILE, file_info["repo_path"])

            parquet_file = os.path.join(current_batch_download_dir, "batch.parquet")
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{str(current_batch_download_dir)}/*.parquet')) TO '{str(parquet_file)}';"
            )

            # Upload to Hugging Face
            api = HfApi()
            repo_id_to_upload = "nhagar/txt360_urls"
            batch_num_str = f"batch_{batch_number}"
            path_in_repo = f"{batch_num_str}.parquet"
            api.create_repo(
                repo_id=repo_id_to_upload,
                exist_ok=True,
                repo_type="dataset",
            )
            print(
                f"Uploading {parquet_file} to {repo_id_to_upload} as {path_in_repo}..."
            )

            api.upload_file(
                path_or_fileobj=parquet_file,
                path_in_repo=path_in_repo,
                repo_id=repo_id_to_upload,
                repo_type="dataset",
                commit_message=f"Add batch {batch_num_str}",
                revision="main",
            )

            print(f"Cleaning up local batch directory: {current_batch_download_dir}")
            # If shutil.rmtree fails (e.g., permissions, file in use), script will exit.
            shutil.rmtree(current_batch_download_dir)
            print(f"Successfully deleted {current_batch_download_dir}.")

            current_batch_files_info = []
            current_batch_size_bytes = 0
            batch_number += 1

    print("\n--- All designated files processed. ---")


if __name__ == "__main__":
    main()
