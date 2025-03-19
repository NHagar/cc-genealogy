import dask.dataframe as dd
from huggingface_hub import HfApi


def repartition_and_upload(dataset, variation, batch_num):
    api = HfApi()
    ddf = dd.read_parquet("data/processed")
    ddf = ddf.repartition(npartitions=1)

    repo_name = f"nhagar/{dataset.split('/')[1]}_urls_{variation}"

    api.create_repo(
        repo_name,
        repo_type="dataset",
        private=False,
        exist_ok=True,
    )

    ddf.to_parquet(
        f"hf://datasets/{repo_name}/batches",
        write_index=False,
        append=False if batch_num == 0 else True,
    )
