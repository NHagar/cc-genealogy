PATTERNS_HF = {
    "c4_en": "hf://datasets/allenai/c4/en/*.json.gz",
    "cultura": "hf://datasets/uonlp/CulturaX/*/*.parquet",
    "falcon": "hf://datasets/tiiuae/falcon-refinedweb/data/*.parquet",
    "fineweb": "hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet",
    "fineweb_edu": "hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet",
    "madlad_cleaned": "hf://datasets/allenai/MADLAD-400/data-v1p5/*/clean*.jsonl.gz",
    "zyda_2": "hf://datasets/Zyphra/Zyda-2/data/**/*.parquet",
    "dclm": "hf://datasets/mlfoundations/dclm-baseline-1.0-parquet/filtered/**/*.parquet",
    "madlad_noisy": "hf://datasets/allenai/MADLAD-400/data-v1p5/*/noisy*.jsonl.gz",
    "c4_en_noblock": "hf://datasets/allenai/c4/en.noblocklist/*.json.gz",
    "c4_multilingual": "hf://datasets/allenai/c4/multilingual/*.json.gz",
    "c4_realnewslike": "hf://datasets/allenai/c4/realnewslike/*.json.gz",
}


def get_dolma_urls():
    with open("./data/dolma_urls.txt", "r") as f:
        data_other = {"dolma": [i.strip() for i in f.readlines()]}

    return data_other
