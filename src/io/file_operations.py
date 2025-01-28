import pandas as pd
from huggingface_hub import HfApi


def create_hf_repo(data: pd.DataFrame, dataset_name: str, token: str) -> None:
    """
    Creates a HuggingFace dataset from a pandas DataFrame.

    Args:
        data (pd.DataFrame): DataFrame to upload
        dataset_name (str): Name to give the dataset on HuggingFace
        token (str): HuggingFace API token
    """
    api = HfApi(token=token)

    api.create_repo(
        dataset_name,
        repo_type="dataset",
        private=False,
    )

    data.to_parquet(f"hf://datasets/{dataset_name}/data", index=False)

    print(f"Successfully uploaded dataset as {dataset_name}")
