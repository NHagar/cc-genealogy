import argparse
import logging
import os
import shutil
import subprocess
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


def setup_logging():
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
        "--num-proc",
        type=int,
        default=4,
        help="Number of processes to use (default: 4)",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default="data/cache",
        help="Directory to use for caching datasets (default: data/cache)",
    )

    args = parser.parse_args()

    # set up caching
    os.makedirs(args.cache_dir, exist_ok=True)

    # Set up logging
    logger = setup_logging()

    logger.info(
        f"Starting processing for dataset: {args.dataset}, variant: {args.variant}"
    )
    logger.debug(
        f"Using batch size: {args.batch_size} bytes, num_proc: {args.num_proc}"
    )

    # Retrieve next batch to process
    logger.info("Retrieving next unprocessed batch")
    batch_num = os.environ.get("SLURM_ARRAY_TASK_ID")
    if batch_num:
        batch_path, _ = retrieve_requested_batch(args.dataset, args.variant, batch_num)
    else:
        batch_path, batch_num = retrieve_next_unprocessed_batch(
            args.dataset, args.variant
        )

    if batch_path is not None:
        logger.info(f"Processing batch {batch_num}")

        logger.debug(f"Loading dataset from batch {batch_num}")

        logger.info(f"Starting download for chunk {batch_num}...")
        aria_command = [
            "aria2c",
            "-i",
            batch_path,  # Input file with URLs
            "-d",
            args.cache_dir,  # Destination directory
            "-c",  # Resume partial downloads
            "--console-log-level=warn",  # Reduce verbosity
            "-j",
            f"{args.num_proc}",  # Number of parallel downloads (adjust based on HPC policy/files)
            # Add other aria2c options as needed
        ]

        try:
            # Run aria2c and wait for it to complete
            result = subprocess.run(
                aria_command, check=True, capture_output=True, text=True
            )
            logger.info(f"aria2c stdout for chunk {batch_num}:\n{result.stdout}")
            logger.info(f"aria2c stderr for chunk {batch_num}:\n{result.stderr}")
            logger.info(f"Download completed successfully for chunk {batch_num}.")

        except subprocess.CalledProcessError as e:
            logger.error(f"Error during aria2c download for chunk {batch_num}!")
            logger.error(f"Return code: {e.returncode}")
            logger.error(f"Stdout: {e.stdout}")
            logger.error(f"Stderr: {e.stderr}")
            # Clean up url list file before exiting?
            # os.remove(url_list_file)
            sys.exit(1)  # Exit the script, Slurm will mark the job task as failed

        # Get URL extraction configuration for this dataset
        suffix = dataset_rules[args.dataset]["variants"][args.variant]["suffix"]
        extraction_sql = dataset_rules[args.dataset]["variants"][args.variant][
            "url_extraction"
        ]
        if suffix == ".parquet":
            file_read_sql = f"READ_PARQUET('{args.cache_dir}/*')"
        else:
            file_read_sql = f"READ_JSON('{args.cache_dir}/*', format = 'newline_delimited', compression='gzip')"

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
        con.execute(
            f"""
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
            ) TO '{args.cache_dir}_processed/batch_{batch_num}.parquet;
        """
        )

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
            path_or_fileobj=f"{args.cache_dir}_processed/batch_{batch_num}.parquet",
            path_in_repo=f"batch_{batch_num}.parquet",
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Add batch {batch_num}",
            revision="main",
        )

        # Clear local cache
        logger.debug(f"Cleaning up cache files in {args.cache_dir}")
        if os.path.exists(args.cache_dir):
            logger.debug(f"Removing {args.cache_dir}")
            shutil.rmtree(args.cache_dir)
        if os.path.exists(f"{args.cache_dir}_processed"):
            logger.debug(f"Removing {args.cache_dir}_processed")
            shutil.rmtree(f"{args.cache_dir}_processed")
        if os.path.exists(batch_path):
            logger.debug(f"Removing {batch_path}")
            os.remove(batch_path)

    logger.info(f"Successfully processed batch {batch_num}")

    con.close()
    logger.debug("Database connection closed")


if __name__ == "__main__":
    main()
