import requests
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
        if file.endswith(".json.gz") and f"{variant}/" in file
    ]

    return paths


def filter_cultura():
    files = list_files("uonlp/CulturaX")

    paths = [
        f"hf://datasets/uonlp/CulturaX/{file}"
        for file in files
        if file.endswith(".parquet")
    ]

    return paths


def filter_falcon():
    files = list_files("tiiuae/falcon-refinedweb")

    paths = [
        f"hf://datasets/tiiuae/falcon-refinedweb/{file}"
        for file in files
        if file.endswith(".parquet")
    ]

    return paths


def filter_fineweb(variant: str):
    files = list_files(f"HuggingFaceFW/{variant}")

    paths = [
        f"hf://datasets/HuggingFaceFW/{variant}/{file}"
        for file in files
        if file.endswith(".parquet") and "data/" in file
    ]

    return paths


def filter_dclm():
    files = list_files("mlfoundations/dclm-baseline-1.0-parquet")

    paths = [
        f"hf://datasets/mlfoundations/dclm-baseline-1.0-parquet/{file}"
        for file in files
        if file.endswith(".parquet")
    ]

    return paths


def filter_zyda_2():
    files = list_files("Zyphra/Zyda-2")

    paths = [
        f"hf://datasets/Zyphra/Zyda-2/{file}"
        for file in files
        if file.endswith(".parquet") and "data/" in file
    ]

    return paths


def filter_madlad(variant: str, quality: str):
    files = list_files("allenai/MADLAD-400")

    paths = [
        f"hf://datasets/allenai/MADLAD-400/{file}"
        for file in files
        if file.endswith(".jsonl.gz")
        and f"{variant}/" in file
        and f"{quality}_" in file
    ]

    return paths


def filter_dolma(variant: str):
    r = requests.get(
        f"https://huggingface.co/datasets/allenai/dolma/raw/main/urls/{variant}.txt"
    )
    paths = r.text.split("\n")

    return paths


COLLECTION_ENUM = {
    "c4_en": lambda: filter_c4("en"),
    "c4_en_noblock": lambda: filter_c4("en.noblocklist"),
    "c4_en_noclean": lambda: filter_c4("en.noclean"),
    "c4_multilingual": lambda: filter_c4("multilingual"),
    "c4_realnewslike": lambda: filter_c4("realnewslike"),
    "cultura": filter_cultura,
    "falcon": filter_falcon,
    "fineweb": lambda: filter_fineweb("fineweb"),
    "fineweb_edu": lambda: filter_fineweb("fineweb-edu"),
    "dclm": filter_dclm,
    "zyda_2": filter_zyda_2,
    "madlad_1_cleaned": lambda: filter_madlad("data", "clean"),
    "madlad_1p5_cleaned": lambda: filter_madlad("data-v1p5", "clean"),
    "madlad_1_noisy": lambda: filter_madlad("data", "noisy"),
    "madlad_1p5_noisy": lambda: filter_madlad("data-v1p5", "noisy"),
    "dolma_v1": lambda: filter_dolma("v1"),
    "dolma_v1p5": lambda: filter_dolma("v1_5"),
    "dolma_v1p6": lambda: filter_dolma("v1_6"),
    "dolma_v1p7": lambda: filter_dolma("v1_7"),
}
