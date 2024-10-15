from pathlib import Path

import duckdb
from huggingface_hub import HfFileSystem
from tqdm import tqdm


def initialize_crawler(pattern: str, skip_existing=True):
    fs = HfFileSystem()
    files = fs.glob(pattern)
    sub_name = "_".join(pattern.split("datasets/")[1].split("/")[0:-1])

    data_path = Path(f"data/{sub_name}")

    if not skip_existing or not data_path.exists():
        data_path.mkdir(parents=True, exist_ok=True)
        to_crawl_path = data_path / "to_crawl.txt"
        seen_path = data_path / "seen.txt"
        error_path = data_path / "error.txt"

        with open(to_crawl_path, "w") as f:
            for file in files:
                f.write(f"{file}\n")

        with open(seen_path, "w") as f:
            pass

        with open(error_path, "w") as f:
            pass

    return data_path


def crawl(data_path, query_path, crawl_errors=False):
    with open(query_path, "r") as f:
        query_template = f.read()

    with open(data_path / "to_crawl.txt", "r") as f:
        to_crawl = f.readlines()

    with open(data_path / "seen.txt", "r") as f:
        seen = f.readlines()

    with open(data_path / "error.txt", "r") as f:
        error = f.readlines()

    con = duckdb.connect(data_path / "database.db")

    if not crawl_errors:
        to_crawl = list(set(to_crawl) - set(seen) - set(error))
    else:
        to_crawl = list(set(error) - set(seen))

    for file in tqdm(to_crawl):
        file = file.strip()
        fname = file.split("/")[-1].replace("-", "_").replace(".", "_")
        try:
            query = query_template.format(fpath=f"hf://{file}")
            q = f"CREATE TABLE {fname} AS ({query})"
            con.execute(q)
            with open(data_path / "seen.txt", "a") as f:
                f.write(f"{file}\n")
        except Exception:
            with open(data_path / "error.txt", "a") as f:
                f.write(f"{file}\n")
