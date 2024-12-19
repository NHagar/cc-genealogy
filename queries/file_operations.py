import glob
import os

import pandas as pd
from datasets import load_dataset


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


def upload_file_to_hf(
    file_path: str,
    dataset_name: str,
    token: str,
    convert_csv_to_parquet: bool = True,
    private: bool = False,
) -> None:
    """
    Uploads a single file as a HuggingFace dataset.

    Args:
        file_path (str): Path to file (parquet or csv)
        dataset_name (str): Name to give the dataset on HuggingFace
        token (str): HuggingFace API token
        convert_csv_to_parquet (bool): Whether to convert CSV to parquet before upload
        private (bool): Whether to make the dataset private
    """
    if convert_csv_to_parquet and file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
        parquet_path = file_path.replace(".csv", ".parquet")
        df.to_parquet(parquet_path, index=False, compression="brotli")
        file_path = parquet_path

    dataset = load_dataset("parquet", data_files=file_path)
    dataset.push_to_hub(dataset_name, token=token, private=private)
    print(f"Successfully uploaded {file_path} as {dataset_name}")
