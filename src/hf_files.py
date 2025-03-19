from typing import List

import duckdb
from huggingface_hub import HfApi

dataset_rules = {
    "multilingual_c4": {
        "prefix": "multilingual/",
        "suffix": ".json.gz",
        "exclude": "-validation",
    }
}


def get_files_for_dataset(dataset: str, ruleset: str) -> List[str]:
    """
    Get the formatted list of filepaths that match the ruleset for a given dataset.

    Args:
        dataset (str): The dataset name to get files for.
        ruleset (str): The ruleset to use for filtering the files.

    Returns:
        List[str]: The list of filepaths that match the ruleset for the dataset.
    """
    api = HfApi()
    repo_files = api.list_repo_files(dataset, repo_type="dataset")
    prefix = dataset_rules[ruleset]["prefix"]
    suffix = dataset_rules[ruleset]["suffix"]
    exclude = dataset_rules[ruleset]["exclude"]
    files = [
        f
        for f in repo_files
        if f.startswith(prefix) and f.endswith(suffix) and exclude not in f
    ]
    files = [f"hf://datasets/{dataset}/{i}" for i in files]

    return files


def get_file_table(dataset: str, ruleset: str):
    """
    Get the formatted table of filepaths that match the ruleset for a given dataset.

    This function creates or retrieves a DuckDB table for the specified dataset and ruleset.
    If the table already exists, it returns the uncollected files as a DataFrame.
    If not, it creates a new table with the filepaths and their collection status.

    Args:
        dataset (str): The dataset name to get files for.
        ruleset (str): The ruleset to use for filtering the files.

    Returns:
        pandas.DataFrame: A DataFrame containing the filepaths and their collection status
                         for files that have not been collected yet.
    """
    con = duckdb.connect("./data/hf_files.db", read_only=False)
    # check if table exists and return if it does
    table_name = f"{dataset}_{ruleset}"
    if table_name in con.table_names():
        return con.execute(
            f"SELECT * FROM {table_name} WHERE collected=false"
        ).fetchdf()

    files = get_files_for_dataset(dataset, ruleset)
    con.execute(f"CREATE TABLE {table_name} (filepath VARCHAR, collected BOOLEAN)")
    con.execute(
        f"INSERT INTO {table_name} VALUES {','.join([f"('{f}', false)" for f in files])}"
    )
    return con.execute(f"SELECT * FROM {table_name} WHERE collected=false").fetchdf()
