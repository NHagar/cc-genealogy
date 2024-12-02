import os
from argparse import ArgumentParser

from dotenv import load_dotenv
from file_operations import combine_parquet_files, upload_directory_to_hf

load_dotenv()

parser = ArgumentParser()
parser.add_argument("--dataset", type=str, required=True)

if __name__ == "__main__":
    args = parser.parse_args()
    directory = f"./data/urls/output/dataset={args.dataset}"
    combine_parquet_files(directory)
    upload_directory_to_hf(
        directory,
        f"nhagar/{args.dataset}_urls",
        token=os.getenv("HF_TOKEN"),
        private=True,
    )
