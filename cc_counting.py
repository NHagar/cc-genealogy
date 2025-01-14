from pathlib import Path

import duckdb
from huggingface_hub import HfApi, snapshot_download
from tqdm import tqdm

con = duckdb.connect()
api = HfApi()

datasets = list(api.list_datasets(author="nhagar"))
ccs = [i for i in datasets if "CC" in i.id]


# make cc directory
Path("./cc").mkdir(exist_ok=True)


def remove_all_contents(directory):
    """Removes all contents of a directory."""

    dir_path = Path(directory)

    if not dir_path.is_dir():
        raise ValueError(f"{directory} is not a directory.")

    for item in dir_path.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            remove_all_contents(item)  # Recursively delete subdirectories


totals = dict()
domain_set = set()

for cc in tqdm(ccs):
    snapshot_download(
        cc.id, repo_type="dataset", local_dir="./cc", allow_patterns=["*.parquet"]
    )
    url_count = con.execute(
        "SELECT SUM(url_count) FROM read_parquet('./cc/data/*.parquet')"
    ).fetchone()[0]
    domains = con.execute(
        "SELECT url_host_name AS domain FROM read_parquet('./cc/data/*.parquet')"
    ).fetch_df()["domain"]

    totals[cc.id] = url_count
    domain_set.update(domains)

    # empty ./cc
    remove_all_contents("./cc")

# write outputs to files
with open("cc_totals.txt", "w") as f:
    f.write(str(totals))

with open("cc_domains.txt", "w") as f:
    f.write("\n".join(domain_set))
