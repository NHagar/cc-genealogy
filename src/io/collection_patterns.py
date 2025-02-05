from huggingface_hub import HfApi


def list_files(repo_name: str):
    api = HfApi()
    files = api.list_repo_files(
        repo_name,
        repo_type="dataset",
    )

    return list(files)


def filter_c4(variant: str):
    files = list_files("allenai/c4")

    paths = [
        f"hf://datasets/allenai/c4/{file}"
        for file in files
        if file.endswith(".json.gz") and variant in file
    ]

    return paths


PATTERNS_HF = {
    "c4_en": filter_c4("en"),
    "c4_en_noblock": filter_c4("en.noblocklist"),
    "c4_en_noclean": filter_c4("en.noclean"),
    "c4_multilingual": filter_c4("multilingual"),
    "c4_realnewslike": filter_c4("realnewslike"),
    "cultura": "hf://datasets/uonlp/CulturaX/*/*.parquet",
    "falcon": "hf://datasets/tiiuae/falcon-refinedweb/data/*.parquet",
    "fineweb": "hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet",
    "fineweb_edu": "hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet",
    "madlad_cleaned": "hf://datasets/allenai/MADLAD-400/data-v1p5/*/clean*.jsonl.gz",
    "zyda_2": "hf://datasets/Zyphra/Zyda-2/data/**/*.parquet",
    "dclm": "hf://datasets/mlfoundations/dclm-baseline-1.0-parquet/filtered/**/*.parquet",
    "madlad_noisy": "hf://datasets/allenai/MADLAD-400/data-v1p5/*/noisy*.jsonl.gz",
}


def get_dolma_urls():
    with open("./data/dolma_urls.txt", "r") as f:
        data_other = {"dolma": [i.strip() for i in f.readlines()]}

    return data_other
