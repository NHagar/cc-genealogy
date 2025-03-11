import concurrent.futures
import json
import logging
import os
import random
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

import datasets
from datasets import Dataset, DownloadConfig, Features, Value, load_dataset
from huggingface_hub import HfApi
from tqdm.auto import tqdm

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
CHECKPOINT_DIR = "checkpoints"


class HFDataPipeline:
    """Pipeline for processing Huggingface datasets with URL data."""

    def __init__(
        self,
        source_repo: str,
        config_name: str = "default",
        batch_size: int = 100_000,
        num_proc: int = 4,
        checkpoint_frequency: int = 5,
        streaming_threshold_gb: float = 1.0,
    ):
        """
        Initialize the pipeline.

        Args:
            source_repo: Source HuggingFace repository ID
            config_name: Configuration name to use from the dataset
            batch_size: Number of samples to process in each batch
            num_proc: Number of processes for parallel operations
            checkpoint_frequency: How often to save checkpoint (in batches)
            streaming_threshold_gb: Size in GB above which to use streaming mode
        """
        self.source_repo = source_repo
        self.config_name = config_name

        if config_name == "default":
            target_repo = f"nhagar/{source_repo.split('/')[-1]}_urls"
        else:
            target_repo = f"nhagar/{source_repo.split('/')[-1]}_urls_{config_name}"

        self.target_repo = target_repo

        self.batch_size = batch_size
        self.num_proc = min(
            num_proc, os.cpu_count() or 4
        )  # Ensure we don't exceed available CPUs
        self.api = HfApi()
        self.checkpoint_frequency = checkpoint_frequency
        self.streaming_threshold_gb = streaming_threshold_gb

        # Create target repository if it doesn't exist
        create_repo(self.target_repo)

        # Ensure checkpoint directory exists
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)

        logger.info(f"Initialized pipeline with {self.num_proc} workers")
        logger.info(f"Using configuration '{self.config_name}'")

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
        logger.info(
            f"Loading dataset from {self.source_repo} (config: {self.config_name}) in streaming mode"
        )

        dlconfig = DownloadConfig(
            max_retries=10,
        )

        dataset = load_dataset(
            self.source_repo,
            self.config_name,
            split="train",
            streaming=True,
            download_config=dlconfig,
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

    @retry_with_exponential_backoff
    def get_next_sample(self, dataset_iterator: Iterator) -> Optional[Dict[str, Any]]:
        """
        Safely get the next sample from a dataset iterator with retry logic.

        Args:
            dataset_iterator: An iterator over a HuggingFace dataset

        Returns:
            The next sample or None if iteration is complete
        """
        try:
            return next(dataset_iterator)
        except StopIteration:
            return None
        # Let other exceptions be caught by the retry decorator

    def save_checkpoint(self, stream_position: int, batch_count: int) -> None:
        """
        Save progress checkpoint to disk.

        Args:
            stream_position: Current position in the stream
            batch_count: Number of batches processed
        """
        checkpoint = {
            "stream_position": stream_position,
            "batch_count": batch_count,
            "timestamp": time.time(),
        }

        checkpoint_file = os.path.join(
            CHECKPOINT_DIR,
            f"{self.source_repo.replace('/', '_')}_{self.config_name}.json",
        )

        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint, f)

        logger.info(
            f"Saved checkpoint at position {stream_position}, batch {batch_count}"
        )

    def load_checkpoint(self) -> Dict[str, Any]:
        """
        Load progress checkpoint from disk if available.

        Returns:
            Dictionary with checkpoint data or default values
        """
        checkpoint_file = os.path.join(
            CHECKPOINT_DIR,
            f"{self.source_repo.replace('/', '_')}_{self.config_name}.json",
        )

        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, "r") as f:
                checkpoint = json.load(f)

            logger.info(
                f"Loaded checkpoint: position {checkpoint['stream_position']}, "
                f"batch {checkpoint['batch_count']}, "
                f"from {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(checkpoint['timestamp']))}"
            )
            return checkpoint

        return {"stream_position": 0, "batch_count": 0}

    def process_dataset(self) -> None:
        """
        Process the entire dataset in batches and upload to target repo using streaming.
        Utilizes parallel processing for batch processing with robust error handling.
        """
        logger.info(
            f"Processing dataset with configuration '{self.config_name}' using streaming mode"
        )

        # Load checkpoint if available
        checkpoint = self.load_checkpoint()
        stream_position = checkpoint["stream_position"]
        batch_count = checkpoint["batch_count"]

        # Initialize tracking variables
        current_batch = []
        batch_to_upload = []
        batches_to_process = []
        future_to_batch = {}

        # Set up executor for parallel processing
        logger.info(f"Setting up parallel processing with {self.num_proc} workers")
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.num_proc)

        # Create progress bar
        pbar = tqdm(desc="Processing batches", unit="batch")

        # Implement chunk-based processing with retries
        streaming_complete = False
        samples_since_checkpoint = 0
        max_consecutive_errors = 20  # Safety limit for consecutive errors
        consecutive_errors = 0

        while not streaming_complete and consecutive_errors < max_consecutive_errors:
            try:
                # Load the dataset in streaming mode
                streaming_dataset = self.load_dataset_streaming()

                # Skip to where we left off
                skipped = 0
                iterator = iter(streaming_dataset)
                while skipped < stream_position:
                    sample = self.get_next_sample(iterator)
                    if sample is None:
                        streaming_complete = True
                        logger.info(
                            f"Reached end of dataset after skipping {skipped} samples"
                        )
                        break
                    skipped += 1

                # Process from current position
                if not streaming_complete:
                    logger.info(f"Resuming from position {stream_position}")
                    consecutive_errors = (
                        0  # Reset error counter on successful iteration
                    )

                    while True:
                        sample = self.get_next_sample(iterator)
                        if sample is None:
                            streaming_complete = True
                            break

                        stream_position += 1
                        samples_since_checkpoint += 1

                        # Only keep the URL from each sample (drop text immediately)
                        try:
                            current_batch.append({"url": sample["url"]})
                        except KeyError as e:
                            logger.warning(
                                f"Sample missing expected fields: {e}. Skipping."
                            )
                            continue

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
                                future_to_batch,
                                batches_to_process,
                                batch_to_upload,
                                pbar,
                            )

                            # Upload periodically to avoid memory issues
                            if len(batch_to_upload) >= self.batch_size * 10:
                                self.upload_batch(batch_to_upload, batch_count)
                                batch_count += 1
                                batch_to_upload = []

                                # Save checkpoint periodically
                                if batch_count % self.checkpoint_frequency == 0:
                                    self.save_checkpoint(stream_position, batch_count)
                                    samples_since_checkpoint = 0

            except (FileNotFoundError, ConnectionError, TimeoutError) as e:
                # Handle streaming errors with retry logic
                consecutive_errors += 1

                backoff = min(
                    MAX_BACKOFF, INITIAL_BACKOFF * (2 ** min(consecutive_errors, 10))
                )
                jitter_amount = random.uniform(-JITTER, JITTER)
                sleep_time = backoff * (1 + jitter_amount)

                logger.warning(
                    f"Streaming error at position {stream_position}: {e}. "
                    f"Retrying in {sleep_time:.2f} seconds... "
                    f"(Consecutive errors: {consecutive_errors})"
                )
                time.sleep(sleep_time)

                # If we've processed a significant number of samples since last checkpoint,
                # save a new checkpoint before retrying
                if samples_since_checkpoint > self.batch_size:
                    self.save_checkpoint(stream_position, batch_count)
                    samples_since_checkpoint = 0

                # Continue the loop to retry loading and streaming
                continue

        # Check if we hit the error limit
        if consecutive_errors >= max_consecutive_errors:
            logger.error(
                f"Exceeded maximum consecutive errors ({max_consecutive_errors}). "
                f"Saving final checkpoint and terminating."
            )
            self.save_checkpoint(stream_position, batch_count)
            raise RuntimeError(
                f"Exceeded maximum consecutive errors: {max_consecutive_errors}"
            )

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

        # Clean up checkpoint after successful completion
        self._cleanup_checkpoint()

        # Shutdown the executor
        executor.shutdown()
        pbar.close()
        logger.info(
            f"Processing complete. Uploaded {batch_count + 1} batches to {self.target_repo}"
        )

    def process_dataset_non_streaming(self) -> None:
        """
        Process the dataset in non-streaming mode for smaller datasets.
        This loads the entire dataset into memory but is more reliable.
        """
        logger.info(
            f"Loading full dataset from {self.source_repo} (config: {self.config_name})"
        )

        try:
            # Use more retries for the download
            dlconfig = DownloadConfig(max_retries=20)

            # Load full dataset with only the needed columns
            dataset = load_dataset(
                self.source_repo,
                self.config_name,
                split="train",
                download_config=dlconfig,
            )

            # Keep only the URL column to save memory
            if "text" in dataset.column_names:
                dataset = dataset.remove_columns(["text"])

            # Process in batches
            logger.info(f"Processing dataset with {len(dataset)} samples")
            batch_count = 0

            # Create progress bar
            pbar = tqdm(
                total=len(dataset) // self.batch_size + 1,
                desc="Processing batches",
                unit="batch",
            )

            for i in range(0, len(dataset), self.batch_size):
                batch = dataset[i : i + self.batch_size]
                processed_batch = self.process_batch(
                    {k: batch[k] for k in batch.column_names}
                )

                # Convert to samples format
                samples = [
                    {k: v[i] for k, v in processed_batch.items()}
                    for i in range(len(processed_batch["url"]))
                ]

                # Upload batch
                self.upload_batch(samples, batch_count)
                batch_count += 1
                pbar.update(1)

            pbar.close()
            logger.info(
                f"Processing complete. {batch_count} batches uploaded to {self.target_repo}"
            )

        except Exception as e:
            logger.error(f"Error in non-streaming processing: {e}")
            raise

    def process(self) -> None:
        """
        Smart processing entry point that chooses the right approach based on dataset size.
        """
        # Try to get dataset info to determine size
        try:
            info = datasets.load_dataset_builder(
                self.source_repo, self.config_name
            ).info

            if hasattr(info, "dataset_size"):
                size_gb = info.dataset_size / (1024 * 1024 * 1024)
                logger.info(f"Dataset size: {size_gb:.2f} GB")

                if size_gb < self.streaming_threshold_gb:
                    logger.info(
                        f"Dataset smaller than threshold ({self.streaming_threshold_gb} GB), using non-streaming mode"
                    )
                    return self.process_dataset_non_streaming()
                else:
                    logger.info(
                        f"Dataset larger than threshold ({self.streaming_threshold_gb} GB), using streaming mode"
                    )
        except Exception as e:
            logger.warning(
                f"Could not determine dataset size: {e}. Defaulting to streaming mode."
            )

        # Default to streaming for large or unknown size
        return self.process_dataset()

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
            data_dir=f"batch_{batch_index}",
        )

    def _cleanup_checkpoint(self) -> None:
        """Remove checkpoint file after successful processing."""
        checkpoint_file = os.path.join(
            CHECKPOINT_DIR,
            f"{self.source_repo.replace('/', '_')}_{self.config_name}.json",
        )

        if os.path.exists(checkpoint_file):
            try:
                os.remove(checkpoint_file)
                logger.info(f"Removed checkpoint file {checkpoint_file}")
            except Exception as e:
                logger.warning(f"Could not remove checkpoint file: {e}")
