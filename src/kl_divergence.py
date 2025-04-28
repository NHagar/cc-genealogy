import subprocess
from pathlib import Path
from typing import Dict, List, Union


def pull_dataset(dataset_name: str) -> Path:
    """
    Pull a dataset from a git repository into the data directory.

    Args:
        dataset_name (str): The name of the dataset (used as directory name).

    Returns:
        Path: Path to the downloaded dataset directory.
    """

    # Create the data directory if it doesn't exist
    data_dir = Path("data")

    # Define the full path to the dataset
    dataset_path = data_dir / dataset_name.split("/")[-1]

    # If the dataset already exists, return its path
    if dataset_path.exists():
        return dataset_path

    # Use git to clone the repository
    try:
        subprocess.run(
            [
                "git",
                "clone",
                f"https://huggingface.co/datasets/{dataset_name}",
                str(dataset_path),
            ],
            check=True,
            capture_output=True,
        )
        print(f"Successfully cloned {dataset_name} into {dataset_path}")
        return dataset_path
    except subprocess.CalledProcessError as e:
        print(f"Failed to clone repository: {e}")
        raise


def calculate_kl_divergence(path1: Path, path2: Path) -> float:
    """
    Calculate the Kullback-Leibler divergence between two datasets.

    Args:
        path1 (Path): Path to the first dataset.
        path2 (Path): Path to the second dataset.

    Returns:
        float: The Kullback-Leibler divergence between the two datasets.
    """
    # Placeholder for actual KL divergence calculation
    return 0.0


def build_query(dataset_metadata: List[Dict]) -> str:
    """
    Build a query string that:
      - Wraps each dataset in a CTE ("subset0", "subset1", …)
      - Unions them all together
      - Aggregates counts per domain across all subsets

    Args:
        dataset_metadata (List[dict]): Each dict must have:
            - "dataset_path": path to the Parquet files (wildcard allowed)
            - "needs_aggregation": bool

    Returns:
        str: The full SQL query.
    """
    # for datasets that haven't been pre-aggregated, count each row as 1
    no_agg = "SELECT url_host_name, url_count FROM '{path}/data/*.parquet'"
    # for datasets that need aggregation, do the count here
    agg = "SELECT domain AS url_host_name, COUNT(*) AS url_count FROM '{path}/*.parquet' GROUP BY 1"

    cte_template = "subset{idx} AS (\n    {select_logic}\n)"
    select_stmts = []
    cte_defs = []

    for idx, ds in enumerate(dataset_metadata):
        template = agg if ds["needs_aggregation"] else no_agg
        select_logic = template.format(path=ds["dataset_path"])
        cte = cte_template.format(idx=idx, select_logic=select_logic)

        # store the per-dataset CTE in the metadata if you need it downstream
        ds["query_cte"] = cte

        cte_defs.append(cte)
        select_stmts.append(f"SELECT * FROM subset{idx}")

    # join all CTE definitions
    cte_section = "WITH\n  " + ",\n  ".join(cte_defs)

    # build the UNION ALL of all subsets
    union_all = "\n  UNION ALL\n  ".join(select_stmts)

    # final roll-up across all subsets
    final_query = f"""{cte_section}
SELECT
  url_host_name,
  SUM(url_count) AS url_count
FROM (
  {union_all}
) AS all_subsets
GROUP BY url_host_name
ORDER BY url_count DESC;"""

    return final_query


def compare_datasets(dataset1: Union[str, List], dataset2: Union[str, List]) -> float:
    """
    Compare two datasets and calculate the Kullback-Leibler divergence.

    Args:
        dataset1 (Union[str, List]): The first dataset or a list of datasets.
        dataset2 (Union[str, List]): The second dataset or a list of datasets.

    Returns:
        float: The Kullback-Leibler divergence between the two datasets.
    """
    if isinstance(dataset1, str):
        dataset1 = [dataset1]
    if isinstance(dataset2, str):
        dataset2 = [dataset2]

    d1_metadata = []
    d2_metadata = []

    for dataset in dataset1:
        dataset_path = pull_dataset(dataset)
        d1_metadata.append(
            {
                "dataset_name": dataset,
                "dataset_path": dataset_path,
                "needs_aggregation": False if "CC-MAIN" in dataset else True,
            }
        )

    for dataset in dataset2:
        dataset_path = pull_dataset(dataset)
        d2_metadata.append(
            {
                "dataset_name": dataset,
                "dataset_path": dataset_path,
                "needs_aggregation": False if "CC-MAIN" in dataset else True,
            }
        )

    # Build the SQL query for each dataset
    query1 = build_query(d1_metadata)
    query2 = build_query(d2_metadata)
    print(f"Query for dataset 1:\n{query1}\n")
    print(f"Query for dataset 2:\n{query2}\n")

    # Placeholder for actual dataset comparison logic
    return 0.0


if __name__ == "__main__":
    compare_datasets(["nhagar/CC-MAIN-2021-17_urls"], ["nhagar/clean_mc4_it_urls"])
