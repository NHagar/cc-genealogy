from argparse import ArgumentParser
from pathlib import Path

import duckdb
import pandas as pd
from tqdm import tqdm

parser = ArgumentParser(description="Consolidate domain data.")
parser.add_argument("--database", help="The database to process")


def get_tables(con):
    return con.execute("SHOW TABLES;").fetchdf()["name"].tolist()


def process_batch(con, tables, batch_size=50):
    result = pd.DataFrame()
    for i in tqdm(range(0, len(tables), batch_size)):
        batch = tables[i : i + batch_size]
        query = " UNION ALL ".join(
            [f"SELECT domain, count FROM {table}" for table in batch]
        )
        df = con.execute(
            f"SELECT domain, SUM(count) as count FROM ({query}) GROUP BY domain"
        ).fetchdf()
        result = pd.concat([result, df]).groupby("domain").sum().reset_index()
    return result


if __name__ == "__main__":
    args = parser.parse_args()
    con = duckdb.connect(database=args.database)

    tables = get_tables(con)
    data = process_batch(con, tables, batch_size=500)

    output_path = Path(args.database).parent / "consolidated.parquet"
    data.to_parquet(str(output_path))
    print(f"Data saved to {output_path}")
