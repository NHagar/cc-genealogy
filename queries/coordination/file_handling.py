import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from huggingface_hub import HfFileSystem
from tqdm import tqdm


def initialize_crawler(pattern: str, skip_existing=True, requires_token=False):
    if requires_token:
        load_dotenv()
        token = os.getenv("HF_TOKEN")
        fs = HfFileSystem(token=token)
    else:
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


def crawl(
    data_path,
    query_path,
    crawl_errors=False,
    is_hf=True,
    requires_token=False,
    stretch_format=False,
):
    with open(query_path, "r") as f:
        query_template = f.read()

    with open(data_path / "to_crawl.txt", "r") as f:
        to_crawl = f.readlines()

    with open(data_path / "seen.txt", "r") as f:
        seen = f.readlines()

    with open(data_path / "error.txt", "r") as f:
        error = f.readlines()

    con = duckdb.connect(str(data_path / "database.db"))

    if not crawl_errors:
        to_crawl = list(set(to_crawl) - set(seen) - set(error))
    else:
        to_crawl = list(set(error) - set(seen))

    if requires_token:
        load_dotenv()
        token = os.getenv("HF_TOKEN")
        con.execute(f"CREATE SECRET hf_token (TYPE HUGGINGFACE, TOKEN '{token}');")

    for file in tqdm(to_crawl):
        file = file.strip()
        if stretch_format:
            fname = "_".join(file.split("/")[-3:]).replace("-", "_").replace(".", "_")
        else:
            fname = file.split("/")[-1].replace("-", "_").replace(".", "_")
        try:
            if is_hf:
                fpath = f"hf://{file}"
            else:
                fpath = file
            query = query_template.format(fpath=fpath)
            q = f"CREATE TABLE a{fname} AS ({query})"
            con.execute(q)
            with open(data_path / "seen.txt", "a") as f:
                f.write(f"{file}\n")
        except Exception:
            with open(data_path / "error.txt", "a") as f:
                f.write(f"{file}\n")
