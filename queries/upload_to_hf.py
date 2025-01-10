import os
from argparse import ArgumentParser

from dotenv import load_dotenv
from file_operations import upload_directory_to_hf

load_dotenv()

parser = ArgumentParser()
parser.add_argument("--dataset", type=str, required=True)
parser.add_argument("--scratch", action="store_true")
parser.add_argument("--large", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()

    token = os.getenv("HF_TOKEN_WRITE")
    if not token:
        raise ValueError("Hugging Face API token not found")

    if args.scratch:
        directory = f"/scratch/nrh146/dataset={args.dataset}"
    else:
        directory = f"./data/urls/output/dataset={args.dataset}"
    upload_directory_to_hf(
        directory,
        f"nhagar/{args.dataset}_urls",
        token=token,
        is_large=args.large,
    )
