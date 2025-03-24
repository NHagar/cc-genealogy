import logging

import duckdb
from huggingface_hub import HfApi

# Set up module logger
logger = logging.getLogger(__name__)

dataset_rules = {
    "allenai/c4": {
        "variants": {
            "multilingual": {
                "prefix": "multilingual/",
                "suffix": ".json.gz",
                "exclude": "-validation",
            }
        }
    },
    "tiiuae/falcon-refinedweb": {
        "variants": {
            "default": {
                "prefix": "data/",
                "suffix": ".parquet",
                "exclude": None,
            }
        }
    },
}


def assign_batches(files, target_batch_size_bytes=100_000_000_000):
    logger.info(
        f"Assigning files to batches with target size {target_batch_size_bytes / 1_000_000_000:.2f} GB"
    )
    logger.debug(f"Total number of files to assign: {len(files)}")

    batches = []
    current_batch = []
    current_batch_size = 0

    # Sort files by size to help create more balanced batches
    logger.debug("Sorting files by size")
    sorted_files = sorted(files, key=lambda x: x.size, reverse=True)

    for file in sorted_files:
        # If adding this file would exceed target batch size and we already have files in the batch
        if current_batch_size + file.size > target_batch_size_bytes and current_batch:
            batches.append(current_batch)
            current_batch = [file]
            current_batch_size = file.size
        else:
            current_batch.append(file)
            current_batch_size += file.size

    # Add the last batch if not empty
    if current_batch:
        batches.append(current_batch)

    # Create a mapping of file path to batch number
    file_to_batch = {}
    for batch_idx, batch in enumerate(batches):
        for file in batch:
            file_to_batch[file.path] = batch_idx

    # Create result with file path and batch number
    result = [(file.path, file_to_batch[file.path], file.size) for file in files]

    # Log batch statistics
    total_size_gb = sum(file.size for file in files) / 1_000_000_000
    logger.info(f"Total size: {total_size_gb:.2f} GB")
    logger.info(f"Number of batches: {len(batches)}")
    logger.info(f"Average batch size: {total_size_gb / len(batches):.2f} GB")

    batch_sizes = {}
    for batch_idx, batch in enumerate(batches):
        batch_size_gb = sum(file.size for file in batch) / 1_000_000_000
        batch_sizes[batch_idx] = batch_size_gb
        logger.debug(f"Batch {batch_idx}: {batch_size_gb:.2f} GB, {len(batch)} files")

    return result


def construct_dataset_tables(
    dataset: str,
    variant: str,
    con: duckdb.DuckDBPyConnection,
    batch_size_bytes: int = 100_000_000_000,
):
    """
    Construct two tables - fpaths and batch status -
    in the DuckDB database for the specified dataset and variant.

    Args:
        dataset (str): The dataset name to construct a table for.
        variant (str): The variant to use for filtering the files.
        con (duckdb.DuckDBPyConnection): The connection to the DuckDB database.
        batch_size_bytes (int, optional): Target batch size in bytes. Defaults to 100GB.
    """
    table_name = f"{dataset.replace('/', '_').replace('-', '_')}_{variant}"
    logger.info(f"Creating tables for {dataset}/{variant}")

    con.execute(f"CREATE TABLE {table_name} (filepath VARCHAR, batch INT)")
    con.execute(f"CREATE TABLE {table_name}_status (batch INT, collected BOOLEAN)")
    logger.debug(f"Created tables {table_name} and {table_name}_status")

    variant_rules = dataset_rules[dataset]["variants"][variant]

    logger.info(f"Fetching repository files from HuggingFace ({dataset})")
    api = HfApi()
    repo_files = api.list_repo_tree(
        dataset,
        repo_type="dataset",
        recursive=True,
        path_in_repo=variant_rules["prefix"],
    )

    repo_files = [i for i in repo_files if i.path.endswith(variant_rules["suffix"])]
    if variant_rules["exclude"] is not None:
        repo_files = [i for i in repo_files if variant_rules["exclude"] not in i.path]

    logger.info(f"Found {len(repo_files)} files matching the criteria")

    logger.info("Assigning files to batches")
    batch_assignments = assign_batches(
        repo_files, target_batch_size_bytes=batch_size_bytes
    )
    batch_nums = list(set([batch for _, batch, _ in batch_assignments]))
    logger.info(f"Created {len(batch_nums)} batches")

    logger.debug(f"Inserting {len(batch_assignments)} file entries into {table_name}")
    con.execute(
        f"INSERT INTO {table_name} VALUES {','.join([f"('{fpath}', {batch})" for fpath, batch, _ in batch_assignments])}"
    )

    logger.debug(
        f"Inserting {len(batch_nums)} batch status entries into {table_name}_status"
    )
    con.execute(
        f"INSERT INTO {table_name}_status VALUES {','.join([f'({batch}, false)' for batch in batch_nums])}"
    )
    logger.info("Successfully populated database tables")


def check_if_dataset_exists(dataset: str, variant: str, con: duckdb.DuckDBPyConnection):
    """
    Check if the specified dataset and variant tables already exist in the DuckDB database.

    Args:
        dataset (str): The dataset name to check for.
        variant (str): The variant to check for.
        con (duckdb.DuckDBPyConnection): The connection to the DuckDB database.

    Returns:
        bool: True if the tables exist, False otherwise.
    """
    table_name = f"{dataset.replace('/', '_').replace('-', '_')}_{variant}"
    logger.debug(f"Checking if {table_name} exists in database")

    tables = con.execute("SHOW TABLES").fetchall()
    tables = [table[0] for table in tables]
    exists = table_name in tables
    logger.debug(f"Table {table_name} {'exists' if exists else 'does not exist'}")
    return exists


def retrieve_next_unprocessed_batch(
    dataset: str, variant: str, con: duckdb.DuckDBPyConnection
):
    """
    Retrieve the next unprocessed batch from the DuckDB database.

    Args:
        dataset (str): The dataset name to retrieve the batch for.
        variant (str): The variant to retrieve the batch for.
        con (duckdb.DuckDBPyConnection): The connection to the DuckDB database.

    Returns:
        tuple: A tuple containing a list of filepaths and the batch number, or None if no batches are left.
    """
    table_name = f"{dataset.replace('/', '_').replace('-', '_')}_{variant}"
    logger.debug(f"Retrieving next unprocessed batch for {dataset}/{variant}")

    result = con.execute(
        f"SELECT batch FROM {table_name}_status WHERE collected = false LIMIT 1"
    )
    first_row = result.fetchone()

    if first_row is None:
        logger.info("No unprocessed batches found. Processing complete.")
        return None, None

    batch_num = first_row[0]
    logger.info(f"Found unprocessed batch: {batch_num}")

    fpaths = con.execute(
        f"SELECT filepath FROM {table_name} WHERE batch = {batch_num}"
    ).fetchall()

    fpaths_list = [fpath[0] for fpath in fpaths]
    logger.debug(f"Batch {batch_num} contains {len(fpaths_list)} files")

    return fpaths_list, batch_num
