from huggingface_hub import HfApi


def create_repo(dataset_name: str):
    api = HfApi()
    api.create_repo(
        dataset_name,
        repo_type="dataset",
        private=False,
        exist_ok=True,
    )
