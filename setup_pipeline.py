import argparse
import logging
import os
import sys

from src.state_tracking import check_if_dataset_exists, write_batch_files


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

    args = parser.parse_args()

    directory_name = f"{args.dataset.replace('/', '_').replace('-', '_').replace('.', '_')}_{args.variant}"

    args = parser.parse_args()
    # Set up logging
    logger = setup_logging()

    logger.info(
        f"Starting processing for dataset: {args.dataset}, variant: {args.variant}"
    )
    logger.debug(f"Using batch size: {args.batch_size} bytes")

    # Check if dataset exists, if not, create it
    if not check_if_dataset_exists(args.dataset, args.variant):
        logger.info(f"Dataset {args.dataset}/{args.variant} not found, creating tables")
        num_batches = write_batch_files(args.dataset, args.variant, args.batch_size)
    else:
        logger.info(f"Dataset {args.dataset}/{args.variant} already exists")
        # Count number of batch files in the directory
        directory_path = os.path.join("data", "batch_files", directory_name)
        batch_files = [f for f in os.listdir(directory_path) if f.endswith(".txt")]
        num_batches = len(batch_files)
        logger.info(f"Found {num_batches} existing batch files in {directory_name}")

    logger.info(
        f"Finished processing for dataset: {args.dataset}, variant: {args.variant}"
    )

    return num_batches


if __name__ == "__main__":
    num_batches = main()
    print("Number of batches creates:")
    print(num_batches)
