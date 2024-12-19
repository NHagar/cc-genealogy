import os
from argparse import ArgumentParser

from dotenv import load_dotenv
from file_operations import upload_directory_to_hf, upload_file_to_hf

load_dotenv()

parser = ArgumentParser()
parser.add_argument("--dataset", type=str, required=True)
parser.add_argument("--is_file", action="store_true")
parser.add_argument("--scratch", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()

    if args.is_file:
        file_path = f"./data/commoncrawl/{args.dataset}.csv"
        upload_file_to_hf(
            file_path,
            f"nhagar/{args.dataset}_urls",
            token=os.getenv("HF_TOKEN_WRITE"),
            private=True,
            convert_csv_to_parquet=True,
        )

    else:
        if args.scratch:
            directory = f"/scratch/nrh146/dataset={args.dataset}"
        else:
            directory = f"./data/urls/output/dataset={args.dataset}"
        upload_directory_to_hf(
            directory,
            f"nhagar/{args.dataset}_urls",
            token=os.getenv("HF_TOKEN_WRITE"),
            private=True,
        )
