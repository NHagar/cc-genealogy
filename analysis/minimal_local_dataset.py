import subprocess
from pathlib import Path

import pandas as pd
import tldextract
from huggingface_hub import HfApi

local_data_location = Path("data/local_dataset")
extension = ".json.gz"
local_files = list(local_data_location.glob(f"*{extension}"))
command = "zstdcat {fpath} | jq -r '.{selector}'"
repo_id = "nhagar/redpajama-data-v2_urls"
selector = "url"


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
    cmd_formatted = command.format(fpath=fpath, selector=selector)
    result = subprocess.run(cmd_formatted, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Failed to process file {fpath}: {result.stderr}")
        raise Exception(f"Failed to process file {fpath}: {result.stderr}")
    extracted_urls = result.stdout.splitlines()
    domains = [extract_domain(url) for url in extracted_urls]

    df = pd.DataFrame({"url": extracted_urls, "domain": domains})
    df = df.dropna()
    df.to_parquet(
        fpath.with_suffix(".parquet"),
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
    # Process each file
    for fpath in local_files:
        try:
            custom_process_file(fpath, selector)
        except Exception as e:
            print(f"Error processing {fpath}: {e}")
            continue
        print(f"Processed {fpath} successfully.")

    # Get all resulting parquet files
    parquet_files = list(local_data_location.glob("*.parquet"))
    # Combine all parquet files into a single dataframe
    combined_df = pd.concat(
        [pd.read_parquet(f, engine="pyarrow") for f in parquet_files], ignore_index=True
    )
    # Save the combined dataframe to a single parquet file
    combined_df.to_parquet(
        local_data_location / "combined_dataset.parquet",
        index=False,
        compression="zstd",
        engine="pyarrow",
    )
    # Upload to Hugging Face
    api = HfApi()
    api.upload_file(
        path_or_fileobj=local_data_location / "combined_dataset.parquet",
        path_in_repo="combined_dataset.parquet",
        repo_id=repo_id,
        repo_type="dataset",
        commit_message="Add combined dataset",
    )
    print("Uploaded combined dataset to Hugging Face.")


if __name__ == "__main__":
    main()
