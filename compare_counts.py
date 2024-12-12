from argparse import ArgumentParser
from pathlib import Path

import duckdb

parser = ArgumentParser()
parser.add_argument("--dataset")

con = duckdb.connect(database=":memory:")

if __name__ == "__main__":
    args = parser.parse_args()
    dataset = args.dataset

    record_count_query = (
        f"SELECT COUNT(*) FROM 'data/urls/to_crawl.csv' WHERE dataset = '{dataset}' "
    )
    record_count = con.execute(record_count_query).fetchone()
    if record_count is None:
        record_count = (0,)

    collected_path = Path(f"data/urls/output/dataset={dataset}")
    collected_count = len(list(collected_path.glob("*.parquet")))

    with open(f"data/urls/{dataset}_count.txt", "w") as f:
        f.write(f"Record count: {record_count[0]}\n")
        f.write(f"Collected count: {collected_count}\n")
        f.write(f"Collected path: {collected_path}")
