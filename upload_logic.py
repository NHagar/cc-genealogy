import os

import dask.dataframe as dd
from dotenv import load_dotenv
from huggingface_hub import HfApi

load_dotenv()

api = HfApi(token=os.getenv("HF_TOKEN_WRITE"))

ddf = dd.read_parquet("data/processed")

ddf = ddf.repartition(npartitions=1)

api.create_repo(
    "nhagar/c4_urls_multilingual",
    repo_type="dataset",
    private=False,
    exist_ok=True,
)

ddf.to_parquet(
    "hf://datasets/nhagar/c4_urls_multilingual/batch/0.parquet", write_index=False
)
