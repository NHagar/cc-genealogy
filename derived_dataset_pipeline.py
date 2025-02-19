from argparse import ArgumentParser

import dask.dataframe as dd
from dask.distributed import Client
from tqdm import tqdm

from src.io.collection_patterns import COLLECTION_ENUM
from src.io.file_operations import read_text_with_retry
from src.orchestration.repo_management import create_repo
from src.transformations.hf_url_processing import get_tld

argparser = ArgumentParser()
argparser.add_argument("--dataset", type=str, required=True)


if __name__ == "__main__":
    args = argparser.parse_args()

    # Start a Dask cluster
    client = Client()
    print(client.dashboard_link)

    # Get paths for collection
    paths = COLLECTION_ENUM[args.dataset]()
    print(f"{len(paths)} paths collected")
    print(paths[:5])
    print(paths[-5:])

    # determine dataset type
    dataset_type = "parquet" if ".parquet" in paths[0] else "json"

    # Create repo
    hf_dataset_name = f"nhagar/{args.dataset}_urls"
    create_repo(hf_dataset_name)

    # NOTE: This batching is done to avoid overwhelming the Dask scheduler on graph creation
    # Split paths into batches of 1000
    batches = [paths[i : i + 1000] for i in range(0, len(paths), 1000)]
    print(f"Split into {len(batches)} batches")

    # Process each batch
    for i, batch in tqdm(enumerate(batches)):
        # Read URLs
        if dataset_type == "parquet":
            data = dd.read_parquet(
                batch,
                columns=["url"],
                aggregate_files=True,
                blocksize="512MB",
            )
        else:
            bag = read_text_with_retry(batch)
            data = bag.to_dataframe(meta={"url": "object"})

        # Extract domain
        data["domain"] = data["url"].apply(get_tld, meta=("url", "object"))

        # Write to repo
        data.to_parquet(
            f"hf://datasets/{hf_dataset_name}/data",
            write_index=False,
            append=False if i == 0 else True,
            name_function=lambda x: f"batch_{i}_part_{x}.parquet",
        )
