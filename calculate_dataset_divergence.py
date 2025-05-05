#!/usr/bin/env python3
"""
Script to calculate KL divergence between pairs of datasets.
Tracks progress and avoids reprocessing already analyzed pairs.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Union

from src.generate_dependency_mapping import pairs
from src.kl_divergence import calculate_dataset_divergence


def setup_logging(log_level=logging.INFO):
    """Set up logging configuration for both file and console output."""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create a unique log filename based on timestamp
    log_file = log_dir / f"kl_divergence_{os.getpid()}.log"

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            # File handler for persistent logs
            logging.FileHandler(log_file),
            # Stream handler for console/Slurm output
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Return the configured logger
    return logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Calculate KL divergence between dataset pairs."
    )
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Run in remote mode (use different storage path)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/dataset_divergence.json",
        help="Path to output file",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
    )
    return parser.parse_args()


def generate_pair_key(
    source: Union[List[str], str], target: Union[List[str], str]
) -> str:
    """
    Generate a human-readable key for a dataset pair.

    Args:
        source: Source dataset(s)
        target: Target dataset(s)

    Returns:
        str: A human-readable key for the pair
    """
    # Convert single dataset to list for uniform handling
    if isinstance(source, str):
        source = [source]
    if isinstance(target, str):
        target = [target]

    # For simplicity, use the last part of the first dataset as source name
    if len(source) == 1:
        source_name = source[0].split("/")[-1]
    else:
        # For multiple sources, use length + partial name of first dataset
        source_name = f"{len(source)}_sources_{source[0].split('/')[-1].split('_')[0]}"

    # Similarly for target
    if len(target) == 1:
        target_name = target[0].split("/")[-1]
    else:
        target_name = f"{len(target)}_targets_{target[0].split('/')[-1].split('_')[0]}"

    return f"{source_name}_to_{target_name}"


def load_completed_pairs(output_path: Path) -> Dict[str, float]:
    """
    Load already processed pairs from the output file.

    Args:
        output_path: Path to the output file

    Returns:
        Dict[str, float]: Dictionary of pair keys to KL divergence values
    """
    if not output_path.exists():
        return {}

    try:
        with open(output_path, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.warning(f"Could not parse {output_path}. Starting with empty results.")
        return {}


def save_results(results: Dict[str, float], output_path: Path):
    """
    Save results to the output file.

    Args:
        results: Dictionary of pair keys to KL divergence values
        output_path: Path to save the results to
    """
    # Ensure the output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Results saved to {output_path}")


def main():
    args = parse_args()

    # Set up logging with the specified log level
    global logger
    log_level = getattr(logging, args.log_level)
    logger = setup_logging(log_level)

    logger.info("Starting KL divergence calculation")
    logger.info(f"Remote mode: {args.remote}")

    output_path = Path(args.output)

    # Load already processed pairs
    completed_results = load_completed_pairs(output_path)
    logger.info(f"Found {len(completed_results)} already processed pairs.")

    # Process all pairs
    for i, (source, target) in enumerate(pairs):
        pair_key = generate_pair_key(source, target)

        # Skip if already processed
        if pair_key in completed_results:
            logger.info(f"Skipping already processed pair: {pair_key}")
            continue

        logger.info(f"Processing pair {i + 1}/{len(pairs)}: {pair_key}")
        logger.info(f"  Source: {source}")
        logger.info(f"  Target: {target}")

        try:
            # Calculate KL divergence between the datasets
            kl_divergence = calculate_dataset_divergence(
                source, target, is_remote=args.remote, cleanup=True
            )

            # Store the result
            completed_results[pair_key] = kl_divergence
            logger.info(f"  KL divergence: {kl_divergence}")

            # Save after each successful calculation
            save_results(completed_results, output_path)

        except Exception as e:
            logger.exception(f"Error processing pair {pair_key}: {e}")

    logger.info(f"Processed {len(completed_results)} pairs in total.")
    logger.info(f"Results saved to {output_path}")


if __name__ == "__main__":
    main()
