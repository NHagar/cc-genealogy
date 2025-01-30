import os
from argparse import ArgumentParser

import dask.dataframe as dd
from dask.distributed import Client, progress
from dotenv import load_dotenv

from src.io.collection_patterns import PATTERNS_HF, get_dolma_urls
from src.io.file_operations import create_hf_repo
from src.transformations.hf_url_processing import safe_get_domain

load_dotenv()

argparser = ArgumentParser()
argparser.add_argument("--dataset", type=str, help="Dataset to crawl")

storage_options = {
    "retries": 5,  # Total attempts (initial + 4 retries)
    "retry_delay": 10,  # Seconds between retries
    "timeout": 60,  # Timeout per request (optional)
}


if __name__ == "__main__":
    # Set up Dask client with dashboard
    client = Client(processes=True)
    print(f"Dashboard link: {client.dashboard_link}")

    args = argparser.parse_args()

    if args.dataset == "dolma":
        input_pattern = get_dolma_urls()
        input_type = "json"
    else:
        input_pattern = PATTERNS_HF[args.dataset]
        input_type = "parquet" if ".parquet" in input_pattern else "json"

    print(f"Reading {args.dataset} URLs from {input_pattern}")

    if input_type == "parquet":
        df = dd.read_parquet(
            input_pattern, columns=["url"], storage_options=storage_options
        )
    else:
        df = dd.read_json(
            input_pattern, compression="gzip", storage_options=storage_options
        )
        df = df["url"]

    df["domain"] = df["url"].map(safe_get_domain, meta=(None, "str"))
    df = df.dropna()
    df["dataset"] = args.dataset

    # Show progress bar during computation
    with progress.ProgressBar():
        df = df.compute()

    create_hf_repo(df, f"nhagar/{args.dataset}_urls", token=os.getenv("HF_TOKEN_WRITE"))

    print("Done!")
