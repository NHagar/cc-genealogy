import concurrent.futures
import hashlib
import os
import threading
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd
from dotenv import load_dotenv
from huggingface_hub import HfFileSystem


class Crawler:
    def __init__(self, crawl_name: str):
        self.crawl_name = crawl_name

        self.query_template_parquet = "SELECT url FROM {fpath}"
        self.query_template_json_var1 = "SELECT url FROM READ_JSON('{fpath}')"
        self.query_template_json_var2 = (
            "SELECT metadata.url AS url FROM READ_JSON('{fpath}')"
        )

        self.data_path = Path(f"data/{self.crawl_name}")
        self.to_crawl_path = self.data_path / "to_crawl.csv"
        self.seen_path = self.data_path / "seen.csv"
        self.error_path = self.data_path / "error.csv"
        self.output_path = (
            self.data_path / "output"
        )  # Changed from output.csv to output directory
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

    def crawl(self, crawl_errors=False):
        """Crawl files"""
        # Read CSVs using pandas
        to_crawl_df = pd.read_csv(self.to_crawl_path)
        seen_df = pd.read_csv(self.seen_path)
        error_df = pd.read_csv(self.error_path)

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
