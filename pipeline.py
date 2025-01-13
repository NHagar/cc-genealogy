from argparse import ArgumentParser

import duckdb

from queries.crawler import initialize_and_run_crawler

parser = ArgumentParser()
parser.add_argument("--crawl-errors", action="store_true")
parser.add_argument("--target-dataset", type=str)

con = duckdb.connect()

# url collection
initialize_and_run_crawler(**vars(parser.parse_args()))

# analysis steps


# data upload
