import argparse
import hashlib
import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def hash_file(file_path):
    file_hash = hashlib.md5(file_path.encode()).hexdigest()[:8]
    return f"{Path(file_path).stem}_{file_hash}.parquet"


def validate_directory(directory_path):
    """
    Validate files in the given directory:
    1. List non-parquet files
    2. Validate parquet files
    """
    # Check if directory exists
    if not os.path.isdir(directory_path):
        logger.error(f"Directory {directory_path} does not exist")
        return

    non_parquet_files = []
    invalid_parquet_files = []

    # Iterate through all files in directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        # Skip if not a file
        if not os.path.isfile(file_path):
            continue

        # Check if file is parquet
        if not filename.endswith(".parquet"):
            non_parquet_files.append(filename)
            continue

        # Validate parquet file
        try:
            # Try to open and read metadata
            pq.read_metadata(file_path)
        except Exception as e:
            logger.error(f"Error validating {filename}: {str(e)}")
            invalid_parquet_files.append(filename)

    # Log results
    if non_parquet_files:
        logger.info("Non-parquet files found:")
        for file in non_parquet_files:
            logger.info(f"- {file}")

    if invalid_parquet_files:
        logger.warning("Invalid parquet files found:")
        for file in invalid_parquet_files:
            logger.warning(f"- {file}")
        logger.info("Updating seen list and removing invalid files...")
        # remove invalid files
        for file in invalid_parquet_files:
            os.remove(os.path.join(directory_path, file))
        logger.info("Invalid files removed")
        # update seen list
        seen = pd.read_csv("data/urls/seen.csv")
        seen["file_encoded"] = seen["file"].apply(hash_file)
        seen_updated = seen[~seen["file_encoded"].isin(invalid_parquet_files)]
        if len(seen) != len(seen_updated):
            logger.info("Updating seen list...")
            seen_updated.to_csv("data/urls/seen.csv", index=False)
            logger.info("Seen list updated")
        else:
            logger.info("No changes in seen list")

    if not non_parquet_files and not invalid_parquet_files:
        logger.info("All files are valid parquet files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate parquet files in a directory"
    )
    parser.add_argument(
        "directory_path", help="Path to the directory containing parquet files"
    )
    args = parser.parse_args()

    validate_directory(args.directory_path)
