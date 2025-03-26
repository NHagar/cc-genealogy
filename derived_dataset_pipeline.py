import argparse
import logging
import os
import shutil
import sys

import duckdb
from datasets import load_dataset

from src.processing import get_tld
from src.state_tracking import (
    check_if_dataset_exists,
    construct_dataset_tables,
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

    # Set up logging
    logger = setup_logging()

    logger.info(
        f"Starting processing for dataset: {args.dataset}, variant: {args.variant}"
    )
    logger.debug(
        f"Using batch size: {args.batch_size} bytes, num_proc: {args.num_proc}"
    )

    # Connect to the database
    logger.info("Connecting to database")
    con = duckdb.connect("data/dataset_status.db")
    logger.debug("Database connection successful")

    # Check if dataset exists, if not, create it
    if not check_if_dataset_exists(args.dataset, args.variant, con):
        logger.info(f"Dataset {args.dataset}/{args.variant} not found, creating tables")
        construct_dataset_tables(args.dataset, args.variant, con, args.batch_size)
    else:
        logger.info(f"Dataset {args.dataset}/{args.variant} already exists")

    # Retrieve next batch to process
    logger.info("Retrieving next unprocessed batch")
    batch, batch_num = retrieve_next_unprocessed_batch(args.dataset, args.variant, con)

    batches_processed = 0
    while batch:
        logger.info(f"Processing batch {batch_num} with {len(batch)} files")

        logger.debug(f"Loading dataset from batch {batch_num}")
        ds = load_dataset(
            args.dataset,
            data_files=batch,
            cache_dir=args.cache_dir,
            # num_proc=args.num_proc,
        )

        logger.debug("Selecting URL column")
        ds = ds.select_columns(["url"])

        logger.debug("Mapping TLD extraction function")
        ds = ds.map(
            get_tld,
            batched=True,
            num_proc=args.num_proc,
            load_from_cache_file=False,
        )

        logger.info(f"Pushing processed batch {batch_num} to HuggingFace Hub")
        ds.push_to_hub(
            repo_id=f"nhagar/{args.dataset.split('/')[1]}_urls_{args.variant}",
            data_dir=f"batch_{batch_num}",
            max_shard_size="1GB",
        )

        # Update the database to mark files as collected
        logger.debug(f"Marking batch {batch_num} as collected in database")
        con.execute(
            f"UPDATE {args.dataset.replace('/', '_')}_{args.variant}_status SET collected = true WHERE batch = {batch_num}"
        )

        # Clear local cache
        logger.debug("Cleaning up cache files")

        # Delete the local cache files
        if os.path.exists(args.cache_dir):
            shutil.rmtree(args.cache_dir)
        if os.path.exists(os.path.expanduser("~/.cache/huggingface/datasets")):
            shutil.rmtree(os.path.expanduser("~/.cache/huggingface/datasets"))
        if os.path.exists(os.path.expanduser("~/.cache/huggingface/hub")):
            shutil.rmtree(os.path.expanduser("~/.cache/huggingface/hub"))

        batches_processed += 1
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
