#!/usr/bin/env python3
"""
HuggingFace Dataset Processing Pipeline
- Loads data from a source HF dataset with 'text' and 'url' columns
- Extracts domain names from URLs
- Handles large datasets with streaming processing
- Implements robust error handling and retries
- Uploads processed data to a new HF repository
"""

import concurrent.futures
import logging
import os
import random
import time
from typing import Any, Callable, Dict, List

import datasets
from datasets import Dataset, Features, Value, load_dataset
from huggingface_hub import HfApi
from tqdm.auto import tqdm

from src.io.collection_patterns import COLLECTION_ENUM
from src.orchestration.repo_management import create_repo
from src.transformations.hf_url_processing import get_tld

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants for retry mechanism
MAX_RETRIES = 5
INITIAL_BACKOFF = 1
MAX_BACKOFF = 60
JITTER = 0.1


class HFDataPipeline:
    """Pipeline for processing Huggingface datasets with URL data."""

    def __init__(
        self,
        source_repo: str,
        batch_size: int = 1000,
        num_proc: int = 4,
    ):
        """
        Initialize the pipeline.

        Args:
            source_repo: Source HuggingFace repository ID
            batch_size: Number of samples to process in each batch
            num_proc: Number of processes for parallel operations
        """
        self.source_repo = source_repo
        self.source_files = COLLECTION_ENUM[source_repo]()
        self.target_repo = f"nhagar/{source_repo}_urls"
        self.batch_size = batch_size
        self.num_proc = min(
            num_proc, os.cpu_count() or 4
        )  # Ensure we don't exceed available CPUs
        self.api = HfApi()

        # Create target repository if it doesn't exist
        create_repo(self.target_repo)

        logger.info(f"Initialized pipeline with {self.num_proc} workers")

    @staticmethod
    def retry_with_exponential_backoff(func: Callable) -> Callable:
        """
        Decorator to implement retry logic with exponential backoff.

        Args:
            func: Function to retry

        Returns:
            Wrapped function with retry logic
        """

        def wrapper(*args, **kwargs):
            retries = 0
            backoff = INITIAL_BACKOFF

            while retries < MAX_RETRIES:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= MAX_RETRIES:
                        logger.error(f"Max retries reached. Last error: {e}")
                        raise

                    # Calculate backoff with jitter
                    jitter_amount = random.uniform(-JITTER, JITTER)
                    sleep_time = min(MAX_BACKOFF, backoff * (2**retries))
                    sleep_time = sleep_time * (1 + jitter_amount)

                    logger.warning(
                        f"Error: {e}. Retrying in {sleep_time:.2f} seconds... "
                        f"(Attempt {retries}/{MAX_RETRIES})"
                    )
                    time.sleep(sleep_time)

        return wrapper

    @retry_with_exponential_backoff
    def load_dataset_streaming(self) -> datasets.IterableDataset:
        """
        Load the dataset in streaming mode to handle large datasets.
        Only loads the necessary columns to minimize memory usage.

        Returns:
            Iterable dataset for streaming processing
        """
        logger.info(f"Loading dataset from {self.source_repo} in streaming mode")

        # Only load the URL column to save memory
        dataset = load_dataset(
            self.source_repo,
            data_files=self.source_files,
            streaming=True,
        )

        return dataset

    @staticmethod
    def extract_domain_batch(urls: List[str]) -> List[str]:
        """
        Extract domain names from a batch of URLs.

        Args:
            urls: List of URLs

        Returns:
            List of domain names
        """
        domains = []
        for url in urls:
            domain = get_tld(url)
            domains.append(domain if domain is not None else "unknown")

        return domains

    def process_batch(self, batch: Dict[str, List]) -> Dict[str, List]:
        """
        Process a batch of data by extracting domains from URLs.
        Drop the memory-intensive 'text' field immediately.

        Args:
            batch: Dictionary with 'url' and possibly 'text' lists

        Returns:
            Processed batch with 'domain' field, without 'text' field
        """
        # Extract domain for each URL in the batch
        domains = self.extract_domain_batch(batch["url"])

        # Create new batch with only url and domain (dropping text)
        processed_batch = {"url": batch["url"], "domain": domains}

        return processed_batch

    def process_dataset(self) -> None:
        """
        Process the entire dataset in batches and upload to target repo.
        Utilizes parallel processing for batch processing.
        """
        # Load the dataset in streaming mode
        streaming_dataset = self.load_dataset_streaming()

        # Initialize batch counter and batches list
        batch_count = 0
        current_batch = []

        # Set up worker pool for parallel processing
        logger.info(f"Setting up parallel processing with {self.num_proc} workers")
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.num_proc)

        # Process the dataset in batches
        logger.info(f"Processing dataset in batches of {self.batch_size}")

        # Create a progress bar that updates periodically
        pbar = tqdm(desc="Processing batches", unit="batch")

        # Process streaming dataset
        batch_to_upload = []
        batches_to_process = []
        future_to_batch = {}

        for sample in streaming_dataset:
            # Only keep the URL from each sample (drop text immediately)
            current_batch.append({"url": sample["url"]})

            # When batch is full, submit it for processing
            if len(current_batch) >= self.batch_size:
                # Convert batch to format expected by process_batch
                batch_dict = {
                    k: [sample[k] for sample in current_batch]
                    for k in current_batch[0].keys()
                }

                # Submit batch for parallel processing
                future = executor.submit(self.process_batch, batch_dict)
                future_to_batch[future] = len(batches_to_process)
                batches_to_process.append(None)  # Placeholder for result

                # Reset current batch
                current_batch = []

                # Check for completed futures
                self._check_and_handle_completed_futures(
                    future_to_batch, batches_to_process, batch_to_upload, pbar
                )

                # Upload periodically to avoid memory issues
                if len(batch_to_upload) >= self.batch_size * 10:
                    self.upload_batch(batch_to_upload, batch_count)
                    batch_count += 1
                    batch_to_upload = []

        # Process any remaining samples
        if current_batch:
            batch_dict = {
                k: [sample[k] for sample in current_batch]
                for k in current_batch[0].keys()
            }
            future = executor.submit(self.process_batch, batch_dict)
            future_to_batch[future] = len(batches_to_process)
            batches_to_process.append(None)

        # Wait for all remaining futures to complete
        logger.info("Waiting for remaining processing tasks to complete...")
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                processed_batch = future.result()
                batches_to_process[batch_idx] = processed_batch

                # Convert to samples format
                samples = [
                    {k: v[i] for k, v in processed_batch.items()}
                    for i in range(len(processed_batch["url"]))
                ]
                batch_to_upload.extend(samples)
                pbar.update(1)
            except Exception as e:
                logger.error(f"Batch processing error: {e}")

        # Upload any remaining processed data
        if batch_to_upload:
            self.upload_batch(batch_to_upload, batch_count)

        # Shutdown the executor
        executor.shutdown()
        pbar.close()
        logger.info(
            f"Processing complete. Uploaded {batch_count + 1} batches to {self.target_repo}"
        )

    def _check_and_handle_completed_futures(
        self,
        future_to_batch: Dict,
        batches_to_process: List,
        batch_to_upload: List,
        pbar: tqdm,
    ) -> None:
        """
        Check for completed futures and handle their results.

        Args:
            future_to_batch: Mapping of futures to batch indices
            batches_to_process: List of processed batches
            batch_to_upload: List of samples ready for upload
            pbar: Progress bar
        """
        # Get completed futures without blocking
        done_futures = [f for f in future_to_batch.keys() if f.done()]

        for future in done_futures:
            batch_idx = future_to_batch.pop(future)
            try:
                processed_batch = future.result()
                batches_to_process[batch_idx] = processed_batch

                # Convert to samples format
                samples = [
                    {k: v[i] for k, v in processed_batch.items()}
                    for i in range(len(processed_batch["url"]))
                ]
                batch_to_upload.extend(samples)
                pbar.update(1)
            except Exception as e:
                logger.error(f"Batch processing error: {e}")

    @retry_with_exponential_backoff
    def upload_batch(self, batch: List[Dict[str, Any]], batch_index: int) -> None:
        """
        Upload a processed batch to the target repository.

        Args:
            batch: List of processed samples
            batch_index: Batch index for filename
        """
        logger.info(f"Uploading batch {batch_index} to {self.target_repo}")

        # Convert batch to Dataset - without text field
        features = Features({"url": Value("string"), "domain": Value("string")})

        dataset = Dataset.from_dict(
            {k: [sample[k] for sample in batch] for k in batch[0].keys()},
            features=features,
        )

        # Push to Hub
        dataset.push_to_hub(
            self.target_repo,
            config_name=f"batch_{batch_index}",
            private=True,
        )


def main():
    """Main entry point with example usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Process a HuggingFace dataset with URLs"
    )
    parser.add_argument(
        "--source-repo", required=True, help="Source HuggingFace repository ID"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )
    parser.add_argument(
        "--num-proc",
        type=int,
        default=4,
        help="Number of processes for parallel operations",
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = HFDataPipeline(
        source_repo=args.source_repo,
        batch_size=args.batch_size,
        num_proc=args.num_proc,
    )

    # Run pipeline
    pipeline.process_dataset()


if __name__ == "__main__":
    main()
