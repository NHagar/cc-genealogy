import argparse
from pathlib import Path

from coordination.file_handling import crawl, initialize_crawler

query_path = Path("queries/domains")

paths = {
    "c4_en": ("hf://datasets/allenai/c4/en/*.json.gz", query_path / "c4_en.sql"),
    "c4_en_noblock": (
        "hf://datasets/allenai/c4/en.noblocklist/*.json.gz",
        query_path / "c4_en_noblocklist.sql",
    ),
    "c4_multilingual": (
        "hf://datasets/allenai/c4/multilingual/*.json.gz",
        query_path / "c4_multilingual.sql",
    ),
    "c4_news": (
        "hf://datasets/allenai/c4/realnewslike/*.json.gz",
        query_path / "c4_realnewslike.sql",
    ),
    "cultura": (
        "hf://datasets/uonlp/CulturaX/*/*.parquet",
        query_path / "culturax.sql",
    ),
    "falcon": (
        "hf://datasets/tiiuae/falcon-refinedweb/data/*.parquet",
        query_path / "falcon_refinedweb.sql",
    ),
    "fineweb": (
        "hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet",
        query_path / "fineweb.sql",
    ),
    "fineweb_edu": (
        "hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet",
        query_path / "fineweb_edu.sql",
    ),
    "madlad_cleaned": (
        "hf://datasets/allenai/MADLAD-400/data-v1p5/*/clean*.jsonl.gz",
        query_path / "madlad_400_cleaned.sql",
    ),
    "madlad_noisy": (
        "hf://datasets/allenai/MADLAD-400/data-v1p5/*/noisy*.jsonl.gz",
        query_path / "madlad_400_raw.sql",
    ),
}

parser = argparse.ArgumentParser(description="Run queries on selected datasets.")
parser.add_argument("--dataset", choices=paths.keys(), help="The dataset to process")

if __name__ == "__main__":
    args = parser.parse_args()
    selected_path = paths[args.dataset]
    print(f"Initializing crawler for {selected_path[0]}")
    data_path = initialize_crawler(selected_path[0], skip_existing=True)
    crawl(data_path, selected_path[1])
