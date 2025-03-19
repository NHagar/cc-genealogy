import argparse

import dask.dataframe as dd
import duckdb
from dask.distributed import Client, LocalCluster

from src.hf_files import get_file_table
from src.processing import get_tld


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Process files from a dataset using a specific variant."
    )
    parser.add_argument(
        "--dataset", type=str, required=True, help="The dataset name to get files for"
    )
    parser.add_argument(
        "--variant",
        type=str,
        required=True,
        help="The variant to use for filtering the files",
    )
    args = parser.parse_args()

    # Set up a Dask local cluster
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask dashboard available at: {client.dashboard_link}")

    # Establish DuckDB connection
    con = duckdb.connect("data/hf_files.db", read_only=False)

    # Get file table using provided arguments
    file_table, table_name = get_file_table(args.dataset, args.variant, con)[:100]
    print(
        f"Found {len(file_table)} files to process for dataset '{args.dataset}' using variant '{args.variant}'"
    )

    # will use this to determine downstream file reads
    is_parquet = True if file_table[0].endswith(".parquet") else False

    ddf = dd.read_json(file_table)
    ddf = ddf[["url"]]
    ddf["tld"] = ddf["url"].apply(get_tld, meta=("url", "object"))

    ddf.to_parquet("data/processed", write_index=False)

    # Update the database to mark files as collected
    for filepath in file_table:
        con.execute(
            f"UPDATE {table_name} SET collected = true WHERE filepath = '{filepath}'"
        )
    print(f"Updated status for {len(file_table)} files in database")

    # Close the DuckDB connection
    con.close()


if __name__ == "__main__":
    main()
