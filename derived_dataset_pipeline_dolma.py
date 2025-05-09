import argparse
import glob  # For finding files
import logging
import os
import sys

import duckdb
from duckdb.typing import VARCHAR
from huggingface_hub import HfApi

# Assuming these custom modules are in a way Python can find them (e.g., in src/ and PYTHONPATH is set or script is run from project root)
from src.processing import extract_domain
from src.state_tracking import (
    dataset_rules,  # retrieve_next_unprocessed_batch, retrieve_requested_batch are no longer needed by this script
)


def setup_logging(output_batch_id_str):
    """Set up logging configuration"""
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger()  # Get root logger
    # Remove existing handlers if any, to avoid duplicate logs if function is called multiple times (though not expected here)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    logger.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    log_filename_suffix = (
        output_batch_id_str if output_batch_id_str else "unknown_batch"
    )
    # Ensure log files are distinct per Slurm job if multiple run for same output_batch_id (e.g. retries)
    # Or, ensure orchestrator uses unique log names for Slurm jobs.
    # The sbatch script already uses %j (SLURM_JOB_ID) for Slurm logs, Python logs are separate.
    python_log_file = f"logs/python_processing_output_batch_{log_filename_suffix}.log"

    file_handler = logging.FileHandler(python_log_file)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def main():
    parser = argparse.ArgumentParser(
        description="Process downloaded dataset files from a cache directory."
    )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
        help="Dataset identifier (e.g., allenai/dolma)",
    )
    parser.add_argument(
        "--variant", type=str, required=True, help="Dataset variant (e.g., v1_5)"
    )
    parser.add_argument(
        "--cache-dir", type=str, required=True, help="Base cache directory."
    )
    parser.add_argument(
        "--current-output-batch-id",  # Renamed for clarity
        type=str,  # Keep as string for flexibility in log name, convert to int if needed for logic
        required=True,
        help="Identifier for the current batch being processed, used for naming outputs and logging.",
    )
    args = parser.parse_args()

    logger = setup_logging(args.current_output_batch_id)
    logger.info(
        f"Python Processing Script Started for Output Batch ID: {args.current_output_batch_id}"
    )
    logger.info(f"  Dataset: {args.dataset}, Variant: {args.variant}")
    logger.info(f"  Cache Directory Base: {args.cache_dir}")

    downloaded_files_dir = os.path.join(
        args.cache_dir, "repo_active_batch"
    )  # Matches orchestrator
    processed_files_dir = os.path.join(args.cache_dir, "processed")
    os.makedirs(processed_files_dir, exist_ok=True)

    logger.info(f"Looking for downloaded files in: {downloaded_files_dir}")

    # Determine file suffix based on dataset rules
    try:
        # Ensure dataset_rules is structured to provide this directly or derive it
        # Example: dataset_rules[args.dataset]["variants"][args.variant]["file_glob_suffix"] could be "*.json.gz"
        # Or just suffix = ".json.gz" if it's fixed for this dataset/variant
        file_glob_pattern = dataset_rules[args.dataset]["variants"][args.variant].get(
            "file_glob_pattern", "*.*"
        )  # Default to all files if not specified
        # Suffix for DuckDB read function
        suffix_for_duckdb = dataset_rules[args.dataset]["variants"][args.variant][
            "suffix"
        ]  # e.g. ".json.gz" or ".parquet"
        extraction_sql_template = dataset_rules[args.dataset]["variants"][args.variant][
            "url_extraction"
        ]
        logger.info(
            f"Using file glob pattern: '{file_glob_pattern}' and suffix for DuckDB: '{suffix_for_duckdb}'"
        )
    except KeyError as e:
        logger.error(
            f"Configuration error in dataset_rules for {args.dataset}/{args.variant}: {e}"
        )
        sys.exit(1)

    # Glob for all relevant files in the download_processing_dir
    # Important: This assumes filenames themselves are sufficient and don't clash if multiple batches
    # were (incorrectly) in this directory. The orchestrator should ensure it's clean for the current batch.
    glob_path = os.path.join(downloaded_files_dir, file_glob_pattern)

    # Get absolute paths from glob
    discovered_files_abs_paths = [os.path.abspath(f) for f in glob.glob(glob_path)]

    if not discovered_files_abs_paths:
        logger.warning(
            f"No files found matching '{glob_path}' in {downloaded_files_dir}. Output parquet for batch {args.current_output_batch_id} will likely be empty or represent no data."
        )
        # Create an empty parquet file with schema to ensure consistency for uploads
        output_parquet_path = os.path.join(
            processed_files_dir, f"batch_{args.current_output_batch_id}.parquet"
        )
        con = duckdb.connect(database=":memory:")
        con.execute(
            "CREATE TABLE urls_data (url VARCHAR, domain VARCHAR);"
        )  # Define schema
        con.execute(f"COPY urls_data TO '{output_parquet_path}' (FORMAT PARQUET);")
        con.close()
        logger.info(f"Created empty parquet with schema at {output_parquet_path}")
    else:
        logger.info(
            f"Found {len(discovered_files_abs_paths)} files to process for batch {args.current_output_batch_id}."
        )
        for f_idx, f_path in enumerate(discovered_files_abs_paths):
            logger.debug(f"  File {f_idx + 1}: {f_path}")

        # Prepare the file paths for the SQL IN clause (DuckDB needs them quoted)
        formatted_paths_for_sql = [f"'{fpath}'" for fpath in discovered_files_abs_paths]
        paths_string_for_sql = ", ".join(formatted_paths_for_sql)

        if suffix_for_duckdb == ".parquet":
            file_read_sql = f"READ_PARQUET([{paths_string_for_sql}])"
        elif ".json" in suffix_for_duckdb:  # Covers .json, .jsonl, .json.gz
            file_read_sql = f"READ_JSON([{paths_string_for_sql}], format = 'newline_delimited', union_by_name=true, filename=true)"
        elif suffix_for_duckdb == ".csv":
            file_read_sql = f"READ_CSV([{paths_string_for_sql}], auto_detect=true, union_by_name=true, filename=true)"
        else:
            logger.error(f"Unsupported file suffix for DuckDB: {suffix_for_duckdb}")
            sys.exit(1)

        final_extraction_sql = extraction_sql_template.replace(
            "{file_read_sql}", file_read_sql
        )
        output_parquet_path = os.path.join(
            processed_files_dir, f"batch_{args.current_output_batch_id}.parquet"
        )

        logger.debug("Connecting to DuckDB and creating UDF...")
        con = duckdb.connect(database=":memory:")
        con.execute("SET enable_progress_bar = true;")
        con.create_function(
            "extract_domain",
            extract_domain,
            [VARCHAR],
            VARCHAR,
            null_handling="special",
        )

        query = f"""
            COPY(
                WITH urls AS (
                    {final_extraction_sql} -- This should be a SELECT ... AS url statement
                )
                SELECT
                    url,
                    extract_domain(url) AS domain
                FROM urls
                WHERE url IS NOT NULL AND trim(url) <> ''
            ) TO '{output_parquet_path}' (FORMAT PARQUET);
        """
        logger.info(f"Executing DuckDB query to create {output_parquet_path}...")
        logger.debug(f"Query: {query}")
        try:
            con.execute(query)
            logger.info("DuckDB query executed successfully.")
        except Exception as e:
            logger.error(
                f"DuckDB query FAILED for output batch ID {args.current_output_batch_id}: {e}"
            )
            logger.error(f"Failed query was: {query}")
            logger.error(f"Files attempted for read: {paths_string_for_sql}")
            con.close()
            sys.exit(1)  # Exit on SQL failure
        finally:
            con.close()  # Ensure connection is closed
            logger.debug("DuckDB connection closed.")

    # Push processed batch to HuggingFace Hub
    if (
        os.path.exists(output_parquet_path) and os.path.getsize(output_parquet_path) > 0
    ):  # Check if parquet exists and is not empty
        logger.info(f"Pushing processed {output_parquet_path} to HuggingFace Hub...")
        api = HfApi()
        dataset_name_part = args.dataset.split("/")[-1]
        repo_id_base = f"nhagar/{dataset_name_part}_urls"
        variant_suffix = ""
        if (
            args.variant
            and args.variant.lower() != "default"
            and args.variant.lower() != "none"
        ):
            variant_suffix = f"_{args.variant.replace('.', '_')}"
        repo_id = f"{repo_id_base}{variant_suffix}"

        logger.debug(f"Target HuggingFace repo ID: {repo_id}")
        try:
            api.create_repo(repo_id=repo_id, exist_ok=True, repo_type="dataset")
            api.upload_file(
                path_or_fileobj=output_parquet_path,
                path_in_repo=f"batch_{args.current_output_batch_id}.parquet",
                repo_id=repo_id,
                repo_type="dataset",
                commit_message=f"Add processed batch {args.current_output_batch_id} for {args.dataset} ({args.variant})",
                revision="main",
            )
            logger.info(
                f"Successfully uploaded to HuggingFace: batch_{args.current_output_batch_id}.parquet"
            )
        except Exception as e:
            logger.error(
                f"FAILED to upload batch {args.current_output_batch_id} to HuggingFace Hub ({repo_id}): {e}"
            )
            # Decide if this is a fatal error for the script. If so: sys.exit(1)
    elif os.path.exists(output_parquet_path):
        logger.info(
            f"Processed parquet file {output_parquet_path} is empty. Skipping HuggingFace upload."
        )
    else:
        logger.warning(
            f"Processed parquet file {output_parquet_path} not found. Cannot upload to HuggingFace Hub."
        )

    # --- CRITICAL STEP: Clear the downloaded files from the active batch directory ---
    logger.info(f"Cleaning up downloaded files from {downloaded_files_dir}...")
    cleaned_count = 0
    if os.path.exists(downloaded_files_dir):  # Check if dir exists
        for f_path in (
            discovered_files_abs_paths
        ):  # Use the list of files it actually found and processed
            if os.path.exists(f_path) and os.path.dirname(
                os.path.abspath(f_path)
            ) == os.path.abspath(downloaded_files_dir):  # Safety check
                try:
                    os.remove(f_path)
                    logger.debug(f"Removed: {f_path}")
                    cleaned_count += 1
                except OSError as e:
                    logger.warning(f"Could not remove downloaded file {f_path}: {e}")
            elif not os.path.exists(f_path):
                logger.debug(
                    f"File already removed or never existed (during cleanup): {f_path}"
                )
            else:
                logger.warning(
                    f"Skipping removal of {f_path} as it's not directly in {downloaded_files_dir}"
                )
        logger.info(f"Cleaned {cleaned_count} files from {downloaded_files_dir}.")
    else:
        logger.warning(
            f"Download directory {downloaded_files_dir} not found during cleanup phase."
        )

    # Clean up the processed parquet file from local cache
    if os.path.exists(output_parquet_path):
        logger.debug(f"Removing local processed parquet: {output_parquet_path}")
        try:
            os.remove(output_parquet_path)
        except OSError as e:
            logger.warning(
                f"Could not remove local processed parquet {output_parquet_path}: {e}"
            )

    logger.info(
        f"Python script finished processing for Output Batch ID: {args.current_output_batch_id}"
    )


if __name__ == "__main__":
    main()
