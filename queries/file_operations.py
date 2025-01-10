import glob
import os

import pandas as pd
from huggingface_hub import HfApi

api = HfApi()


def upload_directory_to_hf(
    directory: str,
    dataset_name: str,
    token: str,
    is_large: bool = True,
    private: bool = False,
) -> None:
    """
    Uploads a directory of parquet files as a HuggingFace dataset.

    Args:
        directory (str): Path to directory containing parquet files
        dataset_name (str): Name to give the dataset on HuggingFace
        token (str, optional): HuggingFace API token. Defaults to None.
        is_large (bool, optional): Whether the dataset is large. Defaults to True.
        private (bool, optional): Whether to make the dataset private. Defaults to False.
    """

    # check for parquet files in directory
    if not glob.glob(os.path.join(directory, "*.parquet")):
        print(f"No parquet files found in {directory}")
        return

    if not is_large:
        api.create_repo(
            f"nhagar/{dataset_name}",
            token=token,
            repo_type="dataset",
            private=private,
        )

        api.upload_folder(
            repo_id=f"nhagar/{dataset_name}",
            folder_path=directory,
            path_in_repo="data",
            token=token,
            repo_type="dataset",
            allow_patterns=["*.parquet"],
        )
    else:
        api.upload_large_folder(
            repo_id=f"nhagar/{dataset_name}",
            folder_path=directory,
            repo_type="dataset",
            allow_patterns=["*.parquet"],
        )

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
