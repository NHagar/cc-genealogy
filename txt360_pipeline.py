import subprocess
from multiprocessing import Pool
from pathlib import Path
from typing import Union

import pandas as pd
import tldextract
from huggingface_hub import HfApi, snapshot_download
from tqdm import tqdm

snapshots = [
    "CC-MAIN-2013-20",
    "CC-MAIN-2013-48",
    "CC-MAIN-2014-10",
    "CC-MAIN-2014-15",
    "CC-MAIN-2014-23",
    "CC-MAIN-2014-35",
    "CC-MAIN-2014-41",
    "CC-MAIN-2014-42",
    "CC-MAIN-2014-49",
    "CC-MAIN-2014-52",
    "CC-MAIN-2015-06",
    "CC-MAIN-2015-11",
    "CC-MAIN-2015-14",
    "CC-MAIN-2015-18",
    "CC-MAIN-2015-22",
    "CC-MAIN-2015-27",
    "CC-MAIN-2015-32",
    "CC-MAIN-2015-35",
    "CC-MAIN-2015-40",
    "CC-MAIN-2015-48",
    "CC-MAIN-2016-07",
    "CC-MAIN-2016-18",
    "CC-MAIN-2016-22",
    "CC-MAIN-2016-26",
    "CC-MAIN-2016-30",
    "CC-MAIN-2016-36",
    "CC-MAIN-2016-40",
    "CC-MAIN-2016-44",
    "CC-MAIN-2016-50",
    "CC-MAIN-2017-04",
    "CC-MAIN-2017-09",
    "CC-MAIN-2017-13",
    "CC-MAIN-2017-17",
    "CC-MAIN-2017-22",
    "CC-MAIN-2017-26",
    "CC-MAIN-2017-30",
    "CC-MAIN-2017-34",
    "CC-MAIN-2017-39",
    "CC-MAIN-2017-43",
    "CC-MAIN-2017-47",
    "CC-MAIN-2017-51",
    "CC-MAIN-2018-05",
    "CC-MAIN-2018-09",
    "CC-MAIN-2018-13",
    "CC-MAIN-2018-17",
    "CC-MAIN-2018-22",
    "CC-MAIN-2018-26",
    "CC-MAIN-2018-30",
    "CC-MAIN-2018-34",
    "CC-MAIN-2018-39",
    "CC-MAIN-2018-43",
    "CC-MAIN-2018-47",
    "CC-MAIN-2018-51",
    "CC-MAIN-2019-04",
    "CC-MAIN-2019-09",
    "CC-MAIN-2019-13",
    "CC-MAIN-2019-18",
    "CC-MAIN-2019-22",
    "CC-MAIN-2019-26",
    "CC-MAIN-2019-30",
    "CC-MAIN-2019-35",
    "CC-MAIN-2019-39",
    "CC-MAIN-2019-43",
    "CC-MAIN-2019-47",
    "CC-MAIN-2019-51",
    "CC-MAIN-2020-05",
    "CC-MAIN-2020-10",
    "CC-MAIN-2020-16",
    "CC-MAIN-2020-24",
    "CC-MAIN-2020-29",
    "CC-MAIN-2020-34",
    "CC-MAIN-2020-40",
    "CC-MAIN-2020-45",
    "CC-MAIN-2020-50",
    "CC-MAIN-2021-04",
    "CC-MAIN-2021-10",
    "CC-MAIN-2021-17",
    "CC-MAIN-2021-21",
    "CC-MAIN-2021-25",
    "CC-MAIN-2021-31",
    "CC-MAIN-2021-39",
    "CC-MAIN-2021-43",
    "CC-MAIN-2021-49",
    "CC-MAIN-2022-05",
    "CC-MAIN-2022-21",
    "CC-MAIN-2022-27",
    "CC-MAIN-2022-33",
    "CC-MAIN-2022-40",
    "CC-MAIN-2022-49",
    "CC-MAIN-2023-06",
    "CC-MAIN-2023-14",
    "CC-MAIN-2023-23",
    "CC-MAIN-2023-40",
    "CC-MAIN-2023-50",
    "CC-MAIN-2024-10",
    "CC-MAIN-2024-18",
    "CC-MAIN-2024-22",
    "CC-MAIN-2024-26",
    "CC-MAIN-2024-30",
]


def download_snapshot(snapshot: str, local_dir: str):
    snapshot_download(
        repo_id="llm360/txt360",
        repo_type="dataset",
        local_dir=local_dir,
        allow_patterns=f"data/common-crawl/{snapshot}/**/*.jsonl.gz",
        max_workers=2,
    )


def extract_domain(url: str) -> Union[str, None]:
    if url is None:
        return None
    try:
        extracted = tldextract.extract(url)
        # Return the full domain information as a formatted string
        return f"{extracted.domain}.{extracted.suffix}"
    except Exception:
        return None  # Handle potential errors in tldextract


def process_url_file(args):
    fpath, selector = args
    command = f"gunzip -c {fpath} | jq -r '.{selector}'"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Failed to process file {fpath}: {result.stderr}")
    extracted_urls = result.stdout.splitlines()
    domains = [extract_domain(url) for url in extracted_urls]

    df = pd.DataFrame({"url": extracted_urls, "domain": domains})
    df = df.dropna()
    df.to_parquet(
        fpath.with_suffix(".parquet"),
        index=False,
        compression="zstd",
        engine="pyarrow",
    )
    return True


def process_urls_parallel(local_dir: str, snapshot: str):
    snapshot_path = f"{local_dir}/data/common-crawl/{snapshot}"
    json_files = list(Path(snapshot_path).glob("**/*.jsonl.gz"))

    with Pool(processes=8) as pool:
        list(
            tqdm(
                pool.imap(
                    process_url_file,
                    [
                        (
                            file,
                            ".meta.url",
                        )
                        for file in json_files
                    ],
                ),
                total=len(json_files),
                desc="Processing files",
            )
        )


def collate_parquet_files(local_dir: str):
    parquet_files = list(Path(local_dir).glob("**/*.parquet"))
    if not parquet_files:
        print("No parquet files found to collate.")
        return

    df_list = []
    for file in tqdm(parquet_files, desc="Reading parquet files"):
        df = pd.read_parquet(file, engine="pyarrow")
        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_parquet(
        Path(local_dir) / "combined_urls.parquet",
        index=False,
        compression="zstd",
        engine="pyarrow",
    )
    print(f"Collated {len(combined_df)} URLs into combined_urls.parquet.")


# upload
def upload_to_hub(local_dir: str, snapshot: str):
    api = HfApi()
    api.create_repo(
        repo_id="nhagar/txt360_urls",
        exist_ok=True,
        repo_type="dataset",
    )
    parquet_path = Path(local_dir) / "combined_urls.parquet"

    print(f"Uploading {parquet_path} to nhagar/txt360_urls as batch_{snapshot}...")

    api.upload_file(
        path_or_fileobj=parquet_path,
        path_in_repo=f"batch_{snapshot}.parquet",
        repo_id="nhagar/txt360_urls",
        repo_type="dataset",
        commit_message="Add combined URLs",
        revision="main",
    )


# cleanup
def cleanup_local_dir(local_dir: str):
    print(f"Cleaning up local directory: {local_dir}")
    for file in Path(local_dir).glob("**/*.parquet"):
        file.unlink()
    for file in Path(local_dir).glob("**/*.jsonl.gz"):
        file.unlink()
    print("Cleanup completed.")


def main(snapshot: str, is_remote: bool = True):
    if is_remote:
        local_dir = "/scratch/nrh146/txt360"
    else:
        local_dir = "./data/txt360"

    print(f"Using local directory: {local_dir}")
    Path(local_dir).mkdir(parents=True, exist_ok=True)
    download_snapshot(snapshot, local_dir)
    process_urls_parallel(local_dir, snapshot)
    collate_parquet_files(local_dir)
    upload_to_hub(local_dir, snapshot)
    cleanup_local_dir(local_dir)
