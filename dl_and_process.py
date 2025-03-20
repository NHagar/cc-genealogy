import argparse

import duckdb
from datasets import load_dataset

from src.processing import get_tld
from src.state_tracking import (
    check_if_dataset_exists,
    construct_dataset_tables,
    retrieve_next_unprocessed_batch,
)


def main():
    parser = argparse.ArgumentParser(description="Download and process dataset batches")
    parser.add_argument(
        "--dataset",
        type=str,
        default="allenai/c4",
        help="Dataset to process (default: allenai/c4)",
    )
    parser.add_argument(
        "--variant",
        type=str,
        default="multilingual",
        help="Dataset variant to process (default: multilingual)",
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

    args = parser.parse_args()

    # Connect to the database
    con = duckdb.connect("data/dataset_status.db")

    # Check if dataset exists, if not, create it
    if not check_if_dataset_exists(args.dataset, args.variant, con):
        construct_dataset_tables(args.dataset, args.variant, con, args.batch_size)

    # Retrieve next batch to process
    batch, batch_num = retrieve_next_unprocessed_batch(args.dataset, args.variant, con)

    while batch:
        ds = load_dataset(
            args.dataset,
            data_files=batch,
            cache_dir="data/cache",
            num_proc=args.num_proc,
        )
        ds = ds.select_columns(["url"])
        ds = ds.map(get_tld, batched=True, num_proc=args.num_proc)
        ds.push_to_hub(
            path=f"nhagar/{args.dataset.split('/')[1]}_urls_{args.variant}",
            data_dir=f"batch_{batch_num}",
            max_shard_size="1GB",
        )
        # Update the database to mark files as collected
        con.execute(
            f"UPDATE {args.dataset.replace('/', '_')}_{args.variant}_status SET collected = true WHERE batch = {batch_num}"
        )
        # Clear local cache
        ds.cleanup_cache_files()

        # Retrieve next batch to process
        batch, batch_num = retrieve_next_unprocessed_batch(
            args.dataset, args.variant, con
        )

    con.close()


if __name__ == "__main__":
    main()
