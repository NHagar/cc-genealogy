from huggingface_hub import HfApi

api = HfApi()
api.delete_folder(
    path_in_repo="batch_4/",
    repo_id="nhagar/dclm-baseline-1.0-parquet_urls",
    repo_type="dataset",
    commit_message="Delete folder",
)
