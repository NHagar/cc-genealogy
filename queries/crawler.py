import os
from pathlib import Path
from typing import List, Union

import duckdb
from dotenv import load_dotenv
from huggingface_hub import HfFileSystem


class Crawler:
    def __init__(self, crawl_name: str):
        self.crawl_name = crawl_name

        self.query_template_parquet = "SELECT url FROM {fpath}"
        self.query_template_json_var1 = "SELECT url FROM READ_JSON('{fpath}')"
        self.query_template_json_var2 = "SELECT metadata.url FROM READ_JSON('{fpath}')"

        self.data_path = Path(f"data/{self.crawl_name}")
        self.to_crawl_path = self.data_path / "to_crawl.txt"
        self.seen_path = self.data_path / "seen.txt"
        self.error_path = self.data_path / "error.txt"
        self.output_path = self.data_path / "output.txt"

    def initialize_crawler(
        self, patterns: List[str], preset_urls: Union[List[str], None] = None
    ):
        """Set up crawler file structure"""
        if preset_urls is None:
            load_dotenv()
            token = os.getenv("HF_TOKEN")
            fs = HfFileSystem(token=token)

            files = []
            for pattern in patterns:
                files += fs.glob(pattern)
            files = [f"hf://{file}" for file in files]
        else:
            files = preset_urls

        self.data_path.mkdir(parents=True, exist_ok=True)

        with open(self.to_crawl_path, "w") as f:
            for file in files:
                f.write(f"{file}\n")

        with open(self.seen_path, "w") as f:
            pass

        with open(self.error_path, "w") as f:
            pass

        with open(self.output_path, "w") as f:
            pass

    def crawl(self, crawl_errors=False):
        """Crawl files"""
        with open(self.to_crawl_path, "r") as f:
            to_crawl = f.readlines()

        with open(self.seen_path, "r") as f:
            seen = f.readlines()

        with open(self.error_path, "r") as f:
            error = f.readlines()

        if not crawl_errors:
            to_crawl = list(set(to_crawl) - set(seen) - set(error))
        else:
            to_crawl = list(set(error) - set(seen))

        con = duckdb.connect(":memory:")

        for file in to_crawl:
            file = file.strip()
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

            if data:
                with open(self.seen_path, "a") as f:
                    f.write(f"{file}\n")

                with open(self.output_path, "a") as f:
                    for row in data:
                        f.write(f"{row[0]}\n")
            else:
                with open(self.error_path, "a") as f:
                    f.write(f"{file}\n")


# TODO: Handle pattern -> dataset mapping
# TODO: Parallelize crawling
# TODO: Set up for URL crawl
# TODO: Clean up repo
