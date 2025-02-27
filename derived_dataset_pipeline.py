import argparse

from src.orchestration.hf_pipeline import HFDataPipeline


def main():
    """Main entry point with example usage."""
    parser = argparse.ArgumentParser(
        description="Process a HuggingFace dataset with URLs"
    )
    parser.add_argument(
        "--source-repo", required=True, help="Source HuggingFace repository ID"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )
    parser.add_argument(
        "--num-proc",
        type=int,
        default=4,
        help="Number of processes for parallel operations",
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = HFDataPipeline(
        source_repo=args.source_repo,
        batch_size=args.batch_size,
        num_proc=args.num_proc,
    )

    # Run pipeline
    pipeline.process_dataset()


if __name__ == "__main__":
    main()
