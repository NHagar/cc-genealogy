import concurrent.futures
import hashlib
import os
import threading
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd
from dotenv import load_dotenv
from huggingface_hub import HfFileSystem, login

patterns_hf = {
    "c4_en": "hf://datasets/allenai/c4/en/*.json.gz",
    "cultura": "hf://datasets/uonlp/CulturaX/*/*.parquet",
    "falcon": "hf://datasets/tiiuae/falcon-refinedweb/data/*.parquet",
    "fineweb": "hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet",
    "fineweb_edu": "hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet",
    "madlad_cleaned": "hf://datasets/allenai/MADLAD-400/data-v1p5/*/clean*.jsonl.gz",
    "zyda_2": "hf://datasets/Zyphra/Zyda-2/data/**/*.parquet",
    "dclm": "hf://datasets/mlfoundations/dclm-baseline-1.0-parquet/filtered/**/*.parquet",
    # "madlad_noisy": "hf://datasets/allenai/MADLAD-400/data-v1p5/*/noisy*.jsonl.gz",
    # "c4_en_noblock": "hf://datasets/allenai/c4/en.noblocklist/*.json.gz",
    # "c4_multilingual": "hf://datasets/allenai/c4/multilingual/*.json.gz",
    # "c4_news": "hf://datasets/allenai/c4/realnewslike/*.json.gz",
}

with open("./data/dolma_urls.txt", "r") as f:
    data_other = {"dolma": [i.strip() for i in f.readlines()]}


class Crawler:
    def __init__(self, crawl_name: str, use_scratch: bool = False):
        self.crawl_name = crawl_name

        self.query_template_parquet = "SELECT url FROM '{fpath}' "
        self.query_template_json_var1 = "SELECT url FROM READ_JSON('{fpath}')"
        self.query_template_json_var2 = (
            "SELECT metadata.url AS url FROM READ_JSON('{fpath}')"
        )

        self.data_path = Path(f"data/{self.crawl_name}")
        self.to_crawl_path = self.data_path / "to_crawl.csv"
        self.seen_path = self.data_path / "seen.csv"
        self.error_path = self.data_path / "error.csv"
        if use_scratch:
            self.output_path = Path("/scratch/nrh146")
        else:
            self.output_path = self.data_path / "output"
        self.lock = threading.Lock()

    def initialize_crawler(self) -> bool:
        """Set up crawler file structure"""
        if self.data_path.exists():
            print(f"Crawler for {self.crawl_name} already initialized, skipping...")
            return False

        self.data_path.mkdir(parents=True, exist_ok=True)
        self.output_path.mkdir(parents=True, exist_ok=True)  # Create output directory

        # Create empty to_crawl.csv with columns
        pd.DataFrame(columns=["dataset", "file"]).to_csv(
            self.to_crawl_path, index=False
        )

        # Create empty seen.csv with columns
        pd.DataFrame(columns=["dataset", "file"]).to_csv(self.seen_path, index=False)

        # Create empty error.csv with columns
        pd.DataFrame(columns=["dataset", "file"]).to_csv(self.error_path, index=False)

        return True

    def populate_hf(self, patterns: Dict[str, str]):
        """Populate data from Hugging Face datasets"""
        load_dotenv()
        token = os.getenv("HF_TOKEN")
        fs = HfFileSystem(token=token)
        for dataset, pattern in patterns.items():
            print(f"Populating {dataset}...")
            files = fs.glob(pattern)
            files = [f"hf://{file}" for file in files]
            df = pd.DataFrame({"dataset": dataset, "file": files})
            df.to_csv(self.to_crawl_path, index=False, mode="a", header=False)

    def populate_other(self, data: Dict[str, List[str]]):
        """Populate data from other sources"""
        for dataset, files in data.items():
            print(f"Populating {dataset}...")
            df = pd.DataFrame({"dataset": dataset, "file": files})
            df.to_csv(self.to_crawl_path, index=False, mode="a", header=False)

    def process_file(self, row):
        file = row["file"]
        dataset = row["dataset"]
        con = duckdb.connect(":memory:")
        load_dotenv()
        token = os.getenv("HF_TOKEN")
        con.execute(f"CREATE SECRET hf_token (TYPE HUGGINGFACE, TOKEN '{token}');")

        if file.endswith(".parquet"):
            query = self.query_template_parquet.format(fpath=file)
            try:
                data = con.execute(query).fetchall()
            except duckdb.BinderException:
                data = []
        else:
            query = self.query_template_json_var1.format(fpath=file)
            try:
                data = con.execute(query).fetchall()
            except duckdb.BinderException:
                query = self.query_template_json_var2.format(fpath=file)
                try:
                    data = con.execute(query).fetchall()
                except duckdb.BinderException:
                    data = []

        con.close()

        with self.lock:
            if data:
                # Append to seen.csv
                pd.DataFrame({"dataset": [dataset], "file": [file]}).to_csv(
                    self.seen_path, mode="a", header=False, index=False
                )
                # Write to partitioned parquet
                output_df = pd.DataFrame(
                    {"dataset": [dataset] * len(data), "url": [row[0] for row in data]}
                )
                partition_path = self.output_path / f"dataset={dataset}"
                partition_path.mkdir(exist_ok=True)

                # Create unique filename using hash
                file_hash = hashlib.md5(file.encode()).hexdigest()[:8]
                output_file = partition_path / f"{Path(file).stem}_{file_hash}.parquet"

                output_df.to_parquet(output_file, index=False, compression="brotli")
            else:
                # Append to error.csv
                pd.DataFrame({"dataset": [dataset], "file": [file]}).to_csv(
                    self.error_path, mode="a", header=False, index=False
                )
            print(f"Processed {file}")

    def crawl(self, crawl_errors=False, target_dataset=None):
        """Crawl files"""
        # Read CSVs using pandas
        to_crawl_df = pd.read_csv(self.to_crawl_path)
        seen_df = pd.read_csv(self.seen_path)
        error_df = pd.read_csv(self.error_path)

        if target_dataset is not None:
            to_crawl_df = to_crawl_df[to_crawl_df["dataset"] == target_dataset]
            seen_df = seen_df[seen_df["dataset"] == target_dataset]
            error_df = error_df[error_df["dataset"] == target_dataset]

        # Filter files to crawl
        if not crawl_errors:
            to_crawl_df = pd.merge(
                to_crawl_df,
                pd.concat([seen_df, error_df]),
                on=["dataset", "file"],
                how="left",
                indicator=True,
            )
            to_crawl_df = to_crawl_df[to_crawl_df["_merge"] == "left_only"][
                ["dataset", "file"]
            ]
        else:
            to_crawl_df = pd.merge(
                error_df, seen_df, on=["dataset", "file"], how="left", indicator=True
            )
            to_crawl_df = to_crawl_df[to_crawl_df["_merge"] == "left_only"][
                ["dataset", "file"]
            ]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.process_file, [row for _, row in to_crawl_df.iterrows()])


def initialize_and_run_crawler(crawl_errors: bool, target_dataset: str):
    login(token=os.getenv("HF_TOKEN"))
    url_crawler = Crawler("urls", use_scratch=True)

    # Initialize the crawler
    print("Initializing crawler...")
    needs_populated = url_crawler.initialize_crawler()
    if needs_populated:
        print("Populating crawler...")
        url_crawler.populate_hf(patterns_hf)
        url_crawler.populate_other(data_other)

    # Run the crawler with command line arguments
    print("Running crawler...")
    url_crawler.crawl(crawl_errors=crawl_errors, target_dataset=target_dataset)
