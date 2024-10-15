from pathlib import Path

from coordination.file_handling import crawl, initialize_crawler

query_path = Path("queries/domains")

paths = [
    ("hf://datasets/allenai/c4/en/*.json.gz", query_path / "c4_en.sql"),
    (
        "hf://datasets/allenai/c4/en.noblocklist/*.json.gz",
        query_path / "c4_en_noblocklist.sql",
    ),
    (
        "hf://datasets/allenai/c4/multilingual/*.json.gz",
        query_path / "c4_multilingual.sql",
    ),
    (
        "hf://datasets/allenai/c4/realnewslike/*.json.gz",
        query_path / "c4_realnewslike.sql",
    ),
    ("hf://datasets/uonlp/CulturaX/*/*.parquet", query_path / "culturax.sql"),
    (
        "hf://datasets/tiiuae/falcon-refinedweb/data/*.parquet",
        query_path / "falcon_refinedweb.sql",
    ),
    (
        "hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet",
        query_path / "fineweb.sql",
    ),
    (
        "hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet",
        query_path / "fineweb_edu.sql",
    ),
    (
        "hf://datasets/allenai/MADLAD-400/data-v1p5/*/clean*.jsonl.gz",
        query_path / "madlad_400_cleaned.sql",
    ),
    (
        "hf://datasets/allenai/MADLAD-400/data-v1p5/*/noisy*.jsonl.gz",
        query_path / "madlad_400_raw.sql",
    ),
]

for p in paths:
    print(f"Initializing crawler for {p[0]}")
    data_path = initialize_crawler(p[0], skip_existing=False)
    crawl(data_path, p[1])
