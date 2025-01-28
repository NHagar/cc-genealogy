import os
from argparse import ArgumentParser

import dask.dataframe as dd
from dotenv import load_dotenv

from src.io.collection_patterns import PATTERNS_HF, get_dolma_urls
from src.io.file_operations import create_hf_repo
from src.transformations.hf_url_processing import safe_get_domain

load_dotenv()

argparser = ArgumentParser()
argparser.add_argument("--dataset", type=str, help="Dataset to crawl")


if __name__ == "__main__":
    args = argparser.parse_args()

    if args.dataset == "dolma":
        input_pattern = get_dolma_urls()
        input_type = "json"
    else:
        input_pattern = PATTERNS_HF[args.dataset]
        input_type = "parquet" if ".parquet" in input_pattern else "json"

    print(f"Reading {args.dataset} URLs from {input_pattern}")

    if input_type == "parquet":
        df = dd.read_parquet(input_pattern, columns=["url"])
    else:
        df = dd.read_json(input_pattern, compression="gzip")
        df = df["url"]

    df["domain"] = df["url"].map(safe_get_domain, meta=(None, "str"))
    df = df.dropna()
    df["dataset"] = args.dataset

    df = df.compute()

    create_hf_repo(df, f"nhagar/{args.dataset}_urls", token=os.getenv("HF_TOKEN_WRITE"))

    print("Done!")
