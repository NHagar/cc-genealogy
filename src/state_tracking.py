import logging
import os

from huggingface_hub import HfApi

# Set up module logger
logger = logging.getLogger(__name__)

dataset_rules = {
    "allenai/c4": {
        "variants": {
            "multilingual": {
                "prefix": ["multilingual/"],
                "suffix": ".json.gz",
                "exclude": ["-validation"],
                "url_extraction": "SELECT url",
            }
        }
    },
    "tiiuae/falcon-refinedweb": {
        "variants": {
            "default": {
                "prefix": ["data/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            }
        }
    },
    "uonlp/culturax": {
        "variants": {
            "default": {
                "prefix": None,
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            }
        }
    },
    "zyphra/zyda": {
        "variants": {
            "default": {
                "prefix": [
                    "data/zyda_no_starcoder/zyda_c4-en/",
                    "data/zyda_no_starcoder/zyda_refinedweb/",
                ],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": """SELECT REGEXP_EXTRACT(source_other, '''url''\s*:\s*''([^'']*)''', 1)""",
            }
        }
    },
    "zyphra/zyda-2": {
        "variants": {
            "dclm_crossdeduped": {
                "prefix": ["data/dclm_crossdeduped/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            },
            "dolma-cc_crossdeduped-filtered": {
                "prefix": ["data/dolma-cc_crossdeduped-filtered/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT id",
            },
            "fwe3": {
                "prefix": ["data/fwe3/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            },
            "zyda_crossdeduped-filtered": {
                "prefix": [
                    "data/zyda_crossdeduped-filtered /zyda_c4-en",
                    "data/zyda_crossdeduped-filtered /zyda_refinedweb",
                ],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": """SELECT REGEXP_EXTRACT(source_other, '''url''\s*:\s*''([^'']*)''', 1)""",
            },
        }
    },
    "zyphra/dclm-dedup": {
        "variants": {
            "default": {
                "prefix": ["data/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            }
        }
    },
    "mlfoundations/dclm-baseline-1.0-parquet": {
        "variants": {
            "default": {
                "prefix": ["filtered/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            }
        }
    },
    "huggingfacefw/fineweb": {
        "variants": {
            "default": {
                "prefix": ["data/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            }
        }
    },
    "huggingfacefw/fineweb-edu": {
        "variants": {
            "default": {
                "prefix": ["data/"],
                "suffix": ".parquet",
                "exclude": None,
                "url_extraction": "SELECT url",
            }
        }
    },
    "huggingfacefw/fineweb-2": {
        "variants": {
            "default": {
                "prefix": ["data/"],
                "suffix": ".parquet",
                "exclude": ["_removed/", "test/"],
                "url_extraction": "SELECT url",
            }
        }
    },
    "allenai/madlad-400": {
        "variants": {
            "clean": {
                "prefix": ["data-v1p5/"],
                "suffix": ".jsonl.gz",
                "exclude": ["noisy_"],
                "url_extraction": "SELECT url",
            },
            "noisy": {
                "prefix": ["data-v1p5/"],
                "suffix": ".jsonl.gz",
                "exclude": ["clean_"],
                "url_extraction": "SELECT url",
            },
        }
    },
    "llm360/txt360": {
        "variants": {
            "default": {
                "prefix": ["data/common-crawl/"],
                "suffix": ".jsonl.gz",
                "exclude": None,
                "url_extraction": "SELECT meta.url",
            }
        }
    },
}


def assign_batches(files, target_batch_size_bytes=100_000_000_000):
    logger.info(
        f"Assigning files to batches with target size {target_batch_size_bytes / 1_000_000_000:.2f} GB"
    )
    logger.debug(f"Total number of files to assign: {len(files)}")

    batches = []
    current_batch = []
    current_batch_size = 0

    # Sort files by size to help create more balanced batches
    logger.debug("Sorting files by size")
    sorted_files = sorted(files, key=lambda x: x.size, reverse=True)

    for file in sorted_files:
        # If adding this file would exceed target batch size and we already have files in the batch
        if current_batch_size + file.size > target_batch_size_bytes and current_batch:
            batches.append(current_batch)
            current_batch = [file]
            current_batch_size = file.size
        else:
            current_batch.append(file)
            current_batch_size += file.size

    # Add the last batch if not empty
    if current_batch:
        batches.append(current_batch)

    # Create a mapping of file path to batch number
    file_to_batch = {}
    for batch_idx, batch in enumerate(batches):
        for file in batch:
            file_to_batch[file.path] = batch_idx + 1  # Start batch numbering from 1

    # Create result with file path and batch number
    result = [(file.path, file_to_batch[file.path], file.size) for file in files]

    # Log batch statistics
    total_size_gb = sum(file.size for file in files) / 1_000_000_000
    logger.info(f"Total size: {total_size_gb:.2f} GB")
    logger.info(f"Number of batches: {len(batches)}")
    logger.info(f"Average batch size: {total_size_gb / len(batches):.2f} GB")

    batch_sizes = {}
    for batch_idx, batch in enumerate(batches):
        batch_size_gb = sum(file.size for file in batch) / 1_000_000_000
        batch_sizes[batch_idx] = batch_size_gb
        logger.debug(f"Batch {batch_idx}: {batch_size_gb:.2f} GB, {len(batch)} files")

    return result


def write_batch_files(
    dataset: str,
    variant: str,
    batch_size_bytes: int = 100_000_000_000,
) -> int:
    """
    Writes directory of batch txt files, one URL per line.

    Args:
        dataset (str): The dataset name to write batch files for.
        variant (str): The variant to use for filtering the files.
        batch_size_bytes (int, optional): Target batch size in bytes. Defaults to 100GB.
    """
    logger.info(f"Writing batch files for {dataset}/{variant}")
    directory_name = f"data/{dataset.replace('/', '_').replace('-', '_').replace('.', '_')}_{variant}"
    logger.info(f"Creating directory {directory_name} for batch files")
    os.makedirs(directory_name, exist_ok=True)
    logger.info(f"Directory {directory_name} created")

    variant_rules = dataset_rules[dataset]["variants"][variant]
    logger.info(f"Fetching repository files from HuggingFace ({dataset})")
    api = HfApi()
    repo_files = api.list_repo_tree(
        dataset,
        repo_type="dataset",
        recursive=True,
    )
    if variant_rules["prefix"] is not None:
        repo_files = [
            i
            for i in repo_files
            if any(i.path.startswith(prefix) for prefix in variant_rules["prefix"])
        ]
    repo_files = [i for i in repo_files if i.path.endswith(variant_rules["suffix"])]
    if variant_rules["exclude"] is not None:
        repo_files = [
            i
            for i in repo_files
            if not any(exclude in i.path for exclude in variant_rules["exclude"])
        ]
    logger.info(f"Found {len(repo_files)} files matching the criteria")
    logger.info("Assigning files to batches")
    batch_assignments = assign_batches(
        repo_files, target_batch_size_bytes=batch_size_bytes
    )
    batch_nums = list(set([batch for _, batch, _ in batch_assignments]))
    logger.info(f"Created {len(batch_nums)} batches")
    logger.debug(
        f"Inserting {len(batch_assignments)} file entries into {directory_name}"
    )
    for batch_num in batch_nums:
        fpaths = [i[0] for i in batch_assignments if i[1] == batch_num]
        out_path = os.path.join(directory_name, f"download_urls_batch_{batch_num}.txt")
        with open(out_path, "w") as f:
            for fp in fpaths:
                f.write(f"{fp}\n")

    logger.info(f"Batch files written to {directory_name}")
    return len(batch_nums)


def check_if_dataset_exists(dataset: str, variant: str):
    """
    Check if the specified dataset and variant tables already exist in the DuckDB database.

    Args:
        dataset (str): The dataset name to check for.
        variant (str): The variant to check for.

    Returns:
        bool: True if the directory exists, False otherwise.
    """
    directory_name = f"data/{dataset.replace('/', '_').replace('-', '_').replace('.', '_')}_{variant}"
    logger.debug(f"Checking if dataset directory {directory_name} exists")
    exists = os.path.exists(directory_name)
    if exists:
        logger.info(f"Dataset directory {directory_name} exists")
    else:
        logger.info(f"Dataset directory {directory_name} does not exist")
    return exists


def retrieve_next_unprocessed_batch(
    dataset: str,
    variant: str,
) -> tuple:
    """
    Retrieve the next unprocessed batch from the batch directory.

    Args:
        dataset (str): The dataset name to retrieve the batch for.
        variant (str): The variant to retrieve the batch for.

    Returns:
        tuple: A tuple containing a filepath and the batch number, or None if no batches are left.
    """
    directory_name = f"data/{dataset.replace('/', '_').replace('-', '_').replace('.', '_')}_{variant}"
    logger.debug(f"Retrieving next unprocessed batch for {dataset}/{variant}")

    # Get all batch files in the directory
    batch_files = [
        f for f in os.listdir(directory_name) if f.startswith("download_urls_batch_")
    ]
    logger.debug(f"Found {len(batch_files)} batch files")
    if not batch_files:
        logger.info("No unprocessed batches found")
        return None, None
    # Sort batch files by batch number
    batch_files.sort(key=lambda x: int(x.split("_")[-1].split(".")[0]))
    logger.debug(f"Sorted batch files: {batch_files}")
    # Get the first unprocessed batch file
    batch_file = batch_files[0]
    batch_num = int(batch_file.split("_")[-1].split(".")[0])
    batch_path = os.path.join(directory_name, batch_file)
    logger.info(f"Next unprocessed batch: {batch_file}")
    logger.debug(f"Batch file path: {batch_path}")
    return batch_path, batch_num


def retrieve_requested_batch(
    dataset: str,
    variant: str,
    batch_num: int,
) -> tuple:
    """
    Retrieve a specific batch from the batch directory.

    Args:
        dataset (str): The dataset name to retrieve the batch for.
        variant (str): The variant to retrieve the batch for.
        batch_num (int): The batch number to retrieve.

    Returns:
        tuple: A tuple containing a filepath and the batch number, or None if no batches are left.
    """
    directory_name = f"data/{dataset.replace('/', '_').replace('-', '_').replace('.', '_')}_{variant}"
    logger.debug(f"Retrieving requested batch {batch_num} for {dataset}/{variant}")

    # Get all batch files in the directory
    batch_files = [
        f for f in os.listdir(directory_name) if f.startswith("download_urls_batch_")
    ]
    logger.debug(f"Found {len(batch_files)} batch files")
    # Sort batch files by batch number
    batch_files.sort(key=lambda x: int(x.split("_")[-1].split(".")[0]))
    logger.debug(f"Sorted batch files: {batch_files}")
    # Get the requested batch file
    batch_file = f"download_urls_batch_{batch_num}.txt"
    if batch_file not in batch_files:
        logger.info(f"Requested batch {batch_num} not found")
        return None, None
    else:
        logger.info(f"Requested batch {batch_num} found")
        return os.path.join(directory_name, batch_file), batch_num
