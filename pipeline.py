from argparse import ArgumentParser
from pathlib import Path

import duckdb

from queries.crawler import initialize_and_run_crawler
from queries.file_operations import initialize_tracking

parser = ArgumentParser()
parser.add_argument("--crawl-errors", action="store_true")
parser.add_argument("--target-dataset", type=str)
parser.add_argument("--upload-hf", action="store_true")

con = duckdb.connect()

pipeline_tracking_path = Path("data/urls/pipeline_tracking.csv")
if not pipeline_tracking_path.exists():
    initialize_tracking()
else:
    pipeline_tracking = con.execute(
        f"SELECT * FROM '{pipeline_tracking_path}'"
    ).fetchdf()


# url collection
initialize_and_run_crawler(**vars(parser.parse_args()))

# analysis steps


# data upload
