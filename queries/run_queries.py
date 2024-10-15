from coordination.file_handling import crawl, initialize_crawler

paths = [
    "hf://datasets/allenai/c4/en/*.json.gz",
    "hf://datasets/allenai/c4/en.noblocklist/*.json.gz",
    "hf://datasets/allenai/c4/multilingual/*.json.gz",
    "hf://datasets/allenai/c4/realnewslike/*.json.gz",
    "hf://datasets/uonlp/CulturaX/*/*.parquet"
    "hf://datasets/tiiuae/falcon-refinedweb/data/*.parquet",
    "hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet",
    "hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet",
    "hf://datasets/allenai/MADLAD-400/data-v1p5/*/clean*.jsonl.gz",
    "hf://datasets/allenai/MADLAD-400/data-v1p5/*/noisy*.jsonl.gz",
]

initialize_crawler()

crawl()
