import argparse

import duckdb

from src.hf_files import get_file_table


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Process files from a dataset using a specific ruleset."
    )
    parser.add_argument(
        "--dataset", type=str, required=True, help="The dataset name to get files for"
    )
    parser.add_argument(
        "--ruleset",
        type=str,
        required=True,
        help="The ruleset to use for filtering the files",
    )
    args = parser.parse_args()

    # Establish DuckDB connection
    con = duckdb.connect("data/hf_files.db", read_only=False)

    # Get file table using provided arguments
    file_table = get_file_table(args.dataset, args.ruleset, con)

    # Print the file table (or do other processing)
    print(
        f"Found {len(file_table)} files to process for dataset '{args.dataset}' using ruleset '{args.ruleset}'"
    )
    print(file_table)

    # Additional processing can be added here


if __name__ == "__main__":
    main()
