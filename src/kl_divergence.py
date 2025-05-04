import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Union

import duckdb


def pull_dataset(dataset_name: str, is_remote: bool = False) -> Path:
    """
    Pull a dataset from a git repository into the data directory.

    Args:
        dataset_name (str): The name of the dataset (used as directory name).

    Returns:
        Path: Path to the downloaded dataset directory.
    """

    # Create the data directory if it doesn't exist
    data_dir = Path("data") if not is_remote else Path("/scratch/nrh146")

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
    ORDER BY url_count DESC"""

    return final_query


def run_query(query: str, output_path: Path, force: bool = False):
    """
    Run a SQL query and save the results to a Parquet file.

    Args:
        query (str): The SQL query to run.
        output_path (Path): Path to save the output Parquet file.
        force (bool): If True, overwrite existing files.

    Returns:
        None
    """
    print(f"Running query:\n{query}\n")
    print(f"Saving results to {output_path}")

    if output_path.exists() and not force:
        print(f"Output file {output_path} already exists. Use force=True to overwrite.")
        return

    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM ({query})) TO '{output_path}' (FORMAT PARQUET)")
    con.close()
    print(f"Query results saved to {output_path}")


def calculate_kl_divergence(path1: Path, path2: Path) -> float:
    """
    Calculate the Kullback-Leibler divergence between two datasets.

    Args:
        path1 (Path): Path to the first dataset.
        path2 (Path): Path to the second dataset.

    Returns:
        float: The Kullback-Leibler divergence between the two datasets.
    """
    query = f"""
    WITH
  P AS (
    SELECT
      url_host_name,
      url_count
      / SUM(url_count) OVER ()            AS p_prob
    FROM '{path1}'
  ),
  Q AS (
    SELECT
      url_host_name,
      url_count
      / SUM(url_count) OVER ()            AS q_prob
    FROM '{path2}'
  )
SELECT
  SUM(
    p_prob
    * LN(
        p_prob
        / COALESCE(q_prob, 1e-12)       -- smoothing to avoid div/0
      )
  ) AS kl_divergence
FROM P
LEFT JOIN Q USING (url_host_name);
"""
    print(f"Running KL divergence query:\n{query}\n")
    con = duckdb.connect()
    result = con.execute(query).fetchone()
    con.close()
    kl_divergence = result[0] if result else 0.0
    return kl_divergence


def calculate_dataset_divergence(
    dataset1: Union[str, List],
    dataset2: Union[str, List],
    is_remote: bool = False,
    cleanup: bool = True,
) -> float:
    """
    Compare two datasets and calculate the Kullback-Leibler divergence.

    Args:
        dataset1 (Union[str, List]): The first dataset or a list of datasets.
        dataset2 (Union[str, List]): The second dataset or a list of datasets.
        cleanup (bool): If True, clean up local files after processing.
            Defaults to True.

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
        dataset_path = pull_dataset(dataset, is_remote=is_remote)
        d1_metadata.append(
            {
                "dataset_name": dataset,
                "dataset_path": dataset_path,
                "needs_aggregation": False
                if "CC-MAIN" in dataset or "CC_MAIN" in dataset
                else True,
            }
        )

    for dataset in dataset2:
        dataset_path = pull_dataset(dataset)
        d2_metadata.append(
            {
                "dataset_name": dataset,
                "dataset_path": dataset_path,
                "needs_aggregation": False
                if "CC-MAIN" in dataset or "CC_MAIN" in dataset
                else True,
            }
        )

    # Build the SQL query for each dataset
    query1 = build_query(d1_metadata)
    query2 = build_query(d2_metadata)
    print(f"Query for dataset 1:\n{query1}\n")
    print(f"Query for dataset 2:\n{query2}\n")

    # Run the queries and save the results
    output_path1 = Path("data") / "dataset1_results.parquet"
    output_path2 = Path("data") / "dataset2_results.parquet"
    run_query(query1, output_path1)
    run_query(query2, output_path2)

    # Calculate the KL divergence
    kl_divergence = calculate_kl_divergence(output_path1, output_path2)
    print(f"KL divergence between {dataset1} and {dataset2}: {kl_divergence}")

    # Clean up local files if requested
    if cleanup:
        for dataset in d1_metadata + d2_metadata:
            dataset_path = Path(dataset["dataset_path"])
            if dataset_path.exists():
                print(f"Cleaning up {dataset_path}")
                shutil.rmtree(dataset_path)
        output_path1.unlink(missing_ok=True)
        output_path2.unlink(missing_ok=True)
        print(f"Cleaned up output files: {output_path1}, {output_path2}")

    return kl_divergence
