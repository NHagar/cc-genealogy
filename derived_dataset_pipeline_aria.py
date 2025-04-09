import argparse
import logging
import os
import shutil
import subprocess
import sys

import duckdb

from src.processing import extract_urls, get_tld
from src.state_tracking import (
    check_if_dataset_exists,
    construct_dataset_tables,
    dataset_rules,
    dump_requested_batch,
    retrieve_next_unprocessed_batch,
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
        "--batch-size",
        type=int,
        default=100_000_000_000,
        help="Target batch size in bytes (default: 100GB)",
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

    dataset_name_clean = (
        args.dataset.replace("/", "_").replace("-", "_").replace(".", "_")
    )

    # Connect to the database
    logger.info("Connecting to database")
    con = duckdb.connect(f"data/{dataset_name_clean}_{args.variant}")
    logger.debug("Database connection successful")

    # Check if dataset exists, if not, create it
    if not check_if_dataset_exists(args.dataset, args.variant, con):
        logger.info(f"Dataset {args.dataset}/{args.variant} not found, creating tables")
        construct_dataset_tables(args.dataset, args.variant, con, args.batch_size)
    else:
        logger.info(f"Dataset {args.dataset}/{args.variant} already exists")

    # Retrieve next batch to process
    logger.info("Retrieving next unprocessed batch")
    batch_num = os.environ.get("SLURM_ARRAY_TASK_ID")
    if batch_num:
        batch_path = dump_requested_batch(args.dataset, args.variant, batch_num, con)
    else:
        batch_path, batch_num = retrieve_next_unprocessed_batch(
            args.dataset, args.variant, con, dump_to_txt=True
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
            args.num_proc,  # Number of parallel downloads (adjust based on HPC policy/files)
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
        extraction_config = dataset_rules[args.dataset]["variants"][args.variant][
            "url_extraction"
        ]
        logger.debug(f"Using URL extraction config: {extraction_config}")

        # Apply flexible URL extraction
        ds = extract_urls(ds, extraction_config, num_proc=args.num_proc)

        logger.debug("Mapping TLD extraction function")
        ds = ds.map(
            get_tld,
            batched=True,
            num_proc=args.num_proc,
            load_from_cache_file=False,
        )

        repo_id = f"nhagar/{args.dataset.split('/')[1]}_urls"

        if args.variant != "default":
            repo_id += f"_{args.variant}"

        logger.info(f"Pushing processed batch {batch_num} to HuggingFace Hub")
        ds.push_to_hub(
            repo_id=repo_id,
            data_dir=f"batch_{batch_num}",
            max_shard_size="1GB",
        )

        # Update the database to mark files as collected
        logger.debug(f"Marking batch {batch_num} as collected in database")
        con.execute(
            f"UPDATE {dataset_name_clean}_{args.variant}_status SET collected = true WHERE batch = {batch_num}"
        )

        # Clear local cache
        logger.debug(f"Cleaning up cache files in {args.cache_dir}")

        if os.path.exists(args.cache_dir):
            datasets_cache_path = os.path.join(args.cache_dir, "datasets")
            hub_cache_path = os.path.join(os.environ.get("HF_HOME"), "hub")
            dl_cache_path = os.path.join(args.cache_dir, "dl")
            if os.path.exists(dl_cache_path):
                logger.debug(f"Removing {dl_cache_path}")
                shutil.rmtree(dl_cache_path)
            if os.path.exists(datasets_cache_path):
                logger.debug(f"Removing {datasets_cache_path}")
                shutil.rmtree(datasets_cache_path)
            if os.path.exists(hub_cache_path):
                logger.debug(f"Removing {hub_cache_path}")
                shutil.rmtree(hub_cache_path)

        logger.info(f"Successfully processed batch {batch_num}")

        # Retrieve next batch to process
        logger.info("Retrieving next unprocessed batch")
        batch, batch_num = retrieve_next_unprocessed_batch(
            args.dataset, args.variant, con
        )

    logger.info(f"Processing complete. Processed {batches_processed} batches.")
    con.close()
    logger.debug("Database connection closed")


if __name__ == "__main__":
    main()
