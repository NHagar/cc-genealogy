import argparse
import logging
import os
import sys

import duckdb
from duckdb.typing import VARCHAR
from huggingface_hub import HfApi

from src.processing import extract_domain
from src.state_tracking import (
    dataset_rules,
    retrieve_next_unprocessed_batch,
    retrieve_requested_batch,
)


def setup_logging(batch_num):
    """Set up logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Configure file handler
    if batch_num:
        file_handler = logging.FileHandler(f"logs/processing_{batch_num}.log")
    else:
        file_handler = logging.FileHandler("logs/processing.log")
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)

    # Configure console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def main():
    batch_num = os.environ.get("SLURM_ARRAY_TASK_ID")
    # Set up logging
    logger = setup_logging(batch_num)
    logger.info("Starting dataset processing script")
    logger.info(f"Batch number: {batch_num}")

    parser = argparse.ArgumentParser(description="Download and process dataset batches")
    parser.add_argument(
        "--dataset",
        type=str,
        help="Dataset to process",
    )
    parser.add_argument(
        "--variant",
        type=str,
        help="Dataset variant to process",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default="data/cache",
        help="Directory to use for caching datasets (default: data/cache)",
    )

    args = parser.parse_args()

    # set up caching
    os.makedirs(f"{args.cache_dir}/processed", exist_ok=True)

    logger.info(
        f"Starting processing for dataset: {args.dataset}, variant: {args.variant}"
    )
    # Retrieve next batch to process
    logger.info("Retrieving next unprocessed batch")
    if batch_num:
        batch_num = int(batch_num)
        batch_path, _ = retrieve_requested_batch(args.dataset, args.variant, batch_num)
    else:
        batch_path, batch_num = retrieve_next_unprocessed_batch(
            args.dataset, args.variant
        )

    if batch_path is not None:
        logger.info(f"Processing batch {batch_num}")

        with open(batch_path, "r") as f:
            fpaths = f.readlines()
        fpaths = [f.strip() for f in fpaths]

        # Get URL extraction configuration for this dataset
        suffix = dataset_rules[args.dataset]["variants"][args.variant]["suffix"]
        extraction_sql = dataset_rules[args.dataset]["variants"][args.variant][
            "url_extraction"
        ]

        # Prepare the file paths for the SQL IN clause
        formatted_paths = [f"'{args.cache_dir}/repo/{fpath}'" for fpath in fpaths]
        paths_string = ", ".join(formatted_paths)

        if suffix == ".parquet":
            file_read_sql = f"READ_PARQUET([{paths_string}])"
        else:
            file_read_sql = f"READ_JSON([{paths_string}], format = 'newline_delimited', compression='gzip')"

        logger.debug("Mapping TLD extraction function")
        con = duckdb.connect(database=":memory:")
        # Register the function as a UDF
        con.create_function(
            "extract_domain",  # Name of the UDF in SQL queries
            extract_domain,  # The Python function to use
            [VARCHAR],  # Input parameter type (URL as string)
            VARCHAR,  # Return type (domain as string)
            null_handling="special",
        )
        q = f"""
            COPY(
            WITH urls AS (
                {extraction_sql} AS url
                FROM {file_read_sql}
            )
            SELECT
                url,
                extract_domain(url) AS domain,
            FROM urls
            WHERE url IS NOT NULL
            ) TO '{args.cache_dir}/processed/batch_{batch_num}.parquet';
        """
        logger.debug(f"Executing SQL query: {q}")
        con.execute(q)

        # Push processed batch to HuggingFace Hub
        logger.info(f"Pushing processed batch {batch_num} to HuggingFace Hub")
        api = HfApi()
        repo_id = f"nhagar/{args.dataset.split('/')[1]}_urls"
        if args.variant != "default":
            repo_id += f"_{args.variant}"
        api.create_repo(
            repo_id=repo_id,
            exist_ok=True,
            repo_type="dataset",
        )
        logger.debug(f"Repo ID: {repo_id}")

        api.upload_file(
            path_or_fileobj=f"{args.cache_dir}/processed/batch_{batch_num}.parquet",
            path_in_repo=f"batch_{batch_num}.parquet",
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Add batch {batch_num}",
            revision="main",
        )

        # Clear local cache
        logger.debug(f"Cleaning up cache files in {args.cache_dir}")
        if os.path.exists(f"{args.cache_dir}/processed/batch_{batch_num}.parquet"):
            logger.debug(
                f"Removing {args.cache_dir}/processed/batch_{batch_num}.parquet"
            )
            os.remove(f"{args.cache_dir}/processed/batch_{batch_num}.parquet")
        if os.path.exists(batch_path):
            logger.debug(f"Removing {batch_path}")
            os.remove(batch_path)

        con.close()
        logger.debug("Database connection closed")

    logger.info(f"Successfully processed batch {batch_num}")


if __name__ == "__main__":
    main()
