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


def assign_batches(files, target_batch_size_bytes=100_000_000_000):
    batches = []
    current_batch = []
    current_batch_size = 0

    # Sort files by size to help create more balanced batches
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

    # Print batch statistics
    total_size_gb = sum(file.size for file in files) / 1_000_000_000
    print(f"Total size: {total_size_gb:.2f} GB")
    print(f"Number of batches: {len(batches)}")
    print(f"Average batch size: {total_size_gb / len(batches):.2f} GB")

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
    table_name = f"{dataset.replace('/', '_')}_{variant}"
    con.execute(f"CREATE TABLE {table_name} (filepath VARCHAR, batch INT)")
    con.execute(f"CREATE TABLE {table_name}_status (batch INT, collected BOOLEAN)")

    variant_rules = dataset_rules[dataset]["variants"][variant]

    api = HfApi()
    repo_files = api.list_repo_tree(
        dataset,
        repo_type="dataset",
        recursive=True,
        path_in_repo=variant_rules["prefix"],
    )
    repo_files = [
        i
        for i in repo_files
        if i.path.endswith(variant_rules["suffix"])
        and variant_rules["exclude"] not in i.path
    ]

    batch_assignments = assign_batches(
        repo_files, target_batch_size_bytes=batch_size_bytes
    )
    batch_nums = list(set([batch for _, batch, _ in batch_assignments]))

    con.execute(
        f"INSERT INTO {table_name} VALUES {','.join([f"('{fpath}', {batch})" for fpath, batch, _ in batch_assignments])}"
    )
    con.execute(
        f"INSERT INTO {table_name}_status VALUES {','.join([f'({batch}, false)' for batch in batch_nums])}"
    )


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
    table_name = f"{dataset.replace('/', '_')}_{variant}"
    tables = con.execute("SHOW TABLES").fetchall()
    return table_name in tables


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
        int: The batch number of the next unprocessed batch.
    """
    table_name = f"{dataset.replace('/', '_')}_{variant}"
    result = con.execute(
        f"SELECT batch FROM {table_name}_status WHERE collected = false LIMIT 1"
    )
    if result.empty:
        print("No unprocessed batches found.")
        return None

    batch_num = result.fetchone()[0]
    print(f"Next unprocessed batch: {batch_num}")
    fpaths = con.execute(
        "SELECT filepath FROM {table_name} WHERE batch = {batch_num}"
    ).fetchall()

    return [fpath[0] for fpath in fpaths], batch_num
