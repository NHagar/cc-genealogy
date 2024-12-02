import glob
import os

import pandas as pd
from datasets import load_dataset


def combine_parquet_files(directory: str, max_size_gb: float = 1.0) -> None:
    """
    Combines parquet files in a directory into consolidated files of specified maximum size.

    Args:
        directory (str): Path to directory containing parquet files
        max_size_gb (float): Maximum size of output files in gigabytes
    """
    # Convert GB to bytes for comparison
    max_size_bytes = max_size_gb * 1024 * 1024 * 1024

    # Get all parquet files in directory
    parquet_files = glob.glob(os.path.join(directory, "*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in {directory}")
        return

    current_chunk = []
    current_size = 0
    output_counter = 1

    for file_path in parquet_files:
        file_size = os.path.getsize(file_path)

        # If adding this file would exceed max size, write current chunk
        if current_size + file_size > max_size_bytes and current_chunk:
            # Combine and write current chunk
            combined_df = pd.concat(
                [pd.read_parquet(f) for f in current_chunk], ignore_index=True
            )
            output_path = os.path.join(directory, f"combined_{output_counter}.parquet")
            combined_df.to_parquet(output_path, index=False)

            # Reset for next chunk
            current_chunk = []
            current_size = 0
            output_counter += 1

        current_chunk.append(file_path)
        current_size += file_size

    # Write remaining files if any
    if current_chunk:
        combined_df = pd.concat(
            [pd.read_parquet(f) for f in current_chunk], ignore_index=True
        )
        output_path = os.path.join(directory, f"combined_{output_counter}.parquet")
        combined_df.to_parquet(output_path, index=False)

    # Remove original files
    for file_path in parquet_files:
        os.remove(file_path)

    print(f"Successfully combined files into {output_counter} chunks")


def upload_directory_to_hf(
    directory: str, dataset_name: str, token: str, private: bool = False
) -> None:
    """
    Uploads a directory of parquet files as a HuggingFace dataset.

    Args:
        directory (str): Path to directory containing parquet files
        dataset_name (str): Name to give the dataset on HuggingFace
        token (str, optional): HuggingFace API token. Defaults to None.
    """

    # list all parquet files in directory
    pq_files = glob.glob(os.path.join(directory, "*.parquet"))

    if not pq_files:
        print(f"No parquet files found in {directory}")
        return

    # Convert to HuggingFace dataset
    dataset = load_dataset("parquet", data_files=pq_files)

    dataset.push_to_hub(dataset_name, token=token, private=private)

    print(f"Successfully uploaded dataset as {dataset_name}")
