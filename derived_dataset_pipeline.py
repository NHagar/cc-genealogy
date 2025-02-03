import os

import dask.dataframe as dd
import tldextract
from dask.distributed import Client
from dotenv import load_dotenv
from huggingface_hub import HfApi
from tqdm import tqdm

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

    os.environ["HF_HUB_ENABLE_TRANSFER"] = "1"
    api = HfApi()

    hf_dataset_name = "nhagar/zyda-2_urls_test"

    api.create_repo(
        hf_dataset_name,
        repo_type="dataset",
        private=False,
        exist_ok=True,
    )

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

    # NOTE: This batching is done to avoid overwhelming the Dask scheduler on graph creation
    # Split paths into batches of 1000
    batches = [paths[i : i + 1000] for i in range(0, len(paths), 1000)]
    print(f"Split into {len(batches)} batches")

    # Process each batch
    for i, batch in tqdm(enumerate(batches), total=len(batches)):
        data = dd.read_parquet(
            batch,
            columns=["url"],
            aggregate_files=True,
            blocksize="2GB",
        )

        data["domain"] = data["url"].apply(get_tld, meta=("url", "object"))

        data.to_parquet(
            f"hf://datasets/{hf_dataset_name}/data",
            write_index=False,
            append=False if i == 0 else True,
            # Set the name_function to avoid duplicate column names
            name_function=lambda x: f"part_{x}_batch_{i}.parquet",
        )
