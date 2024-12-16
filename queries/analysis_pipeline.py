import argparse
import os
from pathlib import Path

import pandas as pd
from crawler import Crawler
from dotenv import load_dotenv
from huggingface_hub import login

load_dotenv()

# Add argument parsing
parser = argparse.ArgumentParser(description="URL collection script")
parser.add_argument("--crawl_errors", action="store_true", help="Crawl error files")
parser.add_argument(
    "--target_dataset", type=str, default=None, help="Target specific dataset"
)
args = parser.parse_args()

login(token=os.getenv("HF_TOKEN"))

patterns_hf = {
    "c4_en": "hf://datasets/nhagar/c4_en_urls/data/*.parquet",
    # "cultura": "hf://datasets/uonlp/CulturaX/*/*.parquet",
    # "falcon": "hf://datasets/tiiuae/falcon-refinedweb/data/*.parquet",
    # "fineweb": "hf://datasets/HuggingFaceFW/fineweb/data/*/*.parquet",
    # "fineweb_edu": "hf://datasets/HuggingFaceFW/fineweb-edu/data/*/*.parquet",
    # "madlad_cleaned": "hf://datasets/allenai/MADLAD-400/data-v1p5/*/clean*.jsonl.gz",
    # "zyda_2": "hf://datasets/Zyphra/Zyda-2/data/**/*.parquet",
    # "dclm": "hf://datasets/mlfoundations/dclm-baseline-1.0-parquet/filtered/**/*.parquet",
    # "dolma": ""
}


def init_and_run_crawler(name, query):
    crawler = Crawler(name)
    crawler.query_template_parquet = query

    # Initialize the crawler
    print("Initializing crawler...")
    needs_populated = crawler.initialize_crawler()
    if needs_populated:
        print("Populating crawler...")
        crawler.populate_hf(patterns_hf)

    # Run the crawler with command line arguments
    print("Running crawler...")
    crawler.crawl(crawl_errors=args.crawl_errors, target_dataset=args.target_dataset)


# Secondary data collection
init_and_run_crawler("toplevel_counts", "SELECT COUNT(*) AS total_rows FROM '{fpath}'")
init_and_run_crawler(
    "domain_counts",
    "SELECT domain, COUNT(*) AS total_rows FROM '{fpath}' GROUP BY domain",
)
init_and_run_crawler(
    "case_studies", "SELECT url FROM '{fpath}' WHERE CONTAINS(url, 'nytimes.com')"
)


# Postprocessing
toplevel_count_files = list(Path("data/toplevel_counts/output").rglob("*.parquet"))
domain_count_files = list(Path("data/domain_counts/output").rglob("*.parquet"))
case_study_files = list(Path("data/case_studies/output").rglob("*.parquet"))

# Sum toplevel counts per dataset
totals = {}
for f in toplevel_count_files:
    dataset = f.parent.name  # Gets the dataset directory name
    df = pd.read_parquet(f)
    total = df["total_rows"].sum()
    totals[dataset] = totals.get(dataset, 0) + total

# Save to CSV
pd.DataFrame(
    {"dataset": list(totals.keys()), "total_rows": list(totals.values())}
).to_csv("data/toplevel_counts/totals.csv", index=False)

# Delete individual files
for f in toplevel_count_files:
    f.unlink()


# Aggregate domain counts per dataset
for dataset in set(f.parent.name for f in domain_count_files):
    dataset_files = [f for f in domain_count_files if f.parent.name == dataset]
    if dataset_files:
        df = pd.concat([pd.read_parquet(f) for f in dataset_files])
        aggregated = df.groupby("domain")["total_rows"].sum().reset_index()
        aggregated.to_csv(f"data/domain_counts/{dataset}_domains.csv", index=False)

# Clean up individual files
for f in domain_count_files:
    f.unlink()

# Concatenate case study files for each dataset
for dataset in set(f.parent.name for f in case_study_files):
    dataset_files = [f for f in case_study_files if f.parent.name == dataset]
    if dataset_files:
        df = pd.concat([pd.read_parquet(f) for f in dataset_files])
        df.to_csv(f"data/case_studies/{dataset}_cases.csv", index=False)

# Clean up individual files
for f in case_study_files:
    f.unlink()
