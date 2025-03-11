import argparse
from typing import List

from datasets import get_dataset_config_names

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
        "--configs",
        nargs="*",
        default=["default"],
        help="Dataset configurations to process. Use 'all' for all available configs.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=100_000, help="Batch size for processing"
    )
    parser.add_argument(
        "--num-proc",
        type=int,
        default=6,
        help="Number of processes for parallel operations",
    )

    args = parser.parse_args()

    # Determine which configurations to process
    configs_to_process: List[str] = []
    if "all" in args.configs:
        try:
            configs_to_process = get_dataset_config_names(args.source_repo)
            print(f"Found {len(configs_to_process)} configurations to process")
        except Exception as e:
            print(f"Error fetching configurations: {e}")
            print("Falling back to 'default' configuration")
            configs_to_process = ["default"]
    else:
        configs_to_process = args.configs

    # Process each configuration
    for config in configs_to_process:
        print(f"Processing configuration: {config}")
        # Initialize pipeline
        pipeline = HFDataPipeline(
            source_repo=args.source_repo,
            config_name=config,
            batch_size=args.batch_size,
            num_proc=args.num_proc,
        )

        # Run pipeline
        pipeline.process_dataset()


if __name__ == "__main__":
    main()
