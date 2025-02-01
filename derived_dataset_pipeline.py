import os

import dask.dataframe as dd
import tldextract
from dask.distributed import Client
from dotenv import load_dotenv
from huggingface_hub import HfApi

load_dotenv()


def get_tld(url):
    return (
        tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if url
        else None
    )


if __name__ == "__main__":
    # Start a Dask cluster
    client = Client()
    print(client.dashboard_link)

    # Create HF client
    api = HfApi(token=os.getenv("HF_TOKEN_WRITE"))
    files = api.list_repo_files(
        "Zyphra/Zyda-2",
        repo_type="dataset",
    )
    paths = [
        f"hf://datasets/Zyphra/Zyda-2/{file}"
        for file in files
        if file.endswith(".parquet") and "data/" in file
    ]

    print(f"{len(paths)} paths collected")

    # Load the data
    data = dd.read_parquet(
        paths[:1_000],
        # "hf://datasets/Zyphra/Zyda-2/data/**/*.parquet",
        columns=["url"],
    )

    data["domain"] = data["url"].apply(get_tld, meta=("url", "object"))

    data.to_parquet("test_data_with_domain", write_index=False)
