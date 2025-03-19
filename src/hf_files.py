from typing import List

import duckdb
from huggingface_hub import HfApi

dataset_rules = {
    "allenai/c4": {
        "variants": {
            "multilingual": {
                "prefix": "multilingual/",
                "suffix": ".json.gz",
                "exclude": "-validation",
            }
        }
    }
}


def get_files_for_dataset(dataset: str, variant: str) -> List[str]:
    """
    Get the formatted list of filepaths that match the variant for a given dataset.

    Args:
        dataset (str): The dataset name to get files for.
        variant (str): The variant to use for filtering the files.

    Returns:
        List[str]: The list of filepaths that match the variant for the dataset.
    """
    api = HfApi()
    repo_files = api.list_repo_files(dataset, repo_type="dataset")
    prefix = dataset_rules[variant]["prefix"]
    suffix = dataset_rules[variant]["suffix"]
    exclude = dataset_rules[variant]["exclude"]
    files = [
        f
        for f in repo_files
        if f.startswith(prefix) and f.endswith(suffix) and exclude not in f
    ]
    files = [f"hf://datasets/{dataset}/{i}" for i in files]

    return files


def get_file_table(dataset: str, variant: str, con: duckdb.DuckDBPyConnection):
    """
    Get the formatted table of filepaths that match the variant for a given dataset.

    This function creates or retrieves a DuckDB table for the specified dataset and variant.
    If the table already exists, it returns the uncollected files as a DataFrame.
    If not, it creates a new table with the filepaths and their collection status.

    Args:
        dataset (str): The dataset name to get files for.
        variant (str): The variant to use for filtering the files.
        con (duckdb.DuckDBPyConnection): The connection to the DuckDB database.

    Returns:
        pandas.DataFrame: A DataFrame containing the filepaths and their collection status
                         for files that have not been collected yet.
    """
    # check if table exists and return if it does
    table_name = f"{dataset.replace('/', '_')}_{variant}"
    tables = con.execute("SHOW TABLES").fetchdf()
    if table_name not in tables["name"].values:
        files = get_files_for_dataset(dataset, variant)
        con.execute(f"CREATE TABLE {table_name} (filepath VARCHAR, collected BOOLEAN)")
        con.execute(
            f"INSERT INTO {table_name} VALUES {','.join([f"('{f}', false)" for f in files])}"
        )
    return (
        con.execute(f"SELECT filepath FROM {table_name} WHERE collected=false")
        .fetchdf()
        .filepath.tolist()
    )
