import logging
import shutil
import subprocess
from pathlib import Path
from typing import List, Union

import duckdb

logger = logging.getLogger(__name__)


class KlDivergence:
    def __init__(
        self,
        dataset1: Union[List[str], str],
        dataset2: Union[List[str], str],
        is_remote: bool = False,
    ):
        """
        Initialize the KlDivergence class with two datasets.

        Args:
            dataset1 (Union[List[str], str]): The first dataset or a list of datasets.
            dataset2 (Union[List[str], str]): The second dataset or a list of datasets.
            is_remote (bool): Used to set data storage location.
        """
        if isinstance(dataset1, str):
            dataset1 = [dataset1]
        if isinstance(dataset2, str):
            dataset2 = [dataset2]

        self.dataset1 = dataset1
        self.dataset2 = dataset2
        self.is_remote = is_remote

        if is_remote:
            self.data_dir = Path("/scratch/nrh146")
        else:
            self.data_dir = Path("data")

    def _pull_dataset(self, dataset_name: str) -> Path:
        """
        Pull a dataset from a git repository into the data directory.

        Args:
            dataset_name (str): The name of the dataset (used as directory name).

        Returns:
            Path: Path to the downloaded dataset directory.
        """
        logger.info(f"Pulling dataset {dataset_name} into {self.data_dir}")

        # Define the full path to the dataset
        dataset_path = self.data_dir / dataset_name.split("/")[-1]

        # If the dataset already exists, return its path
        if dataset_path.exists():
            return dataset_path

        # Use git to clone the repository
        try:
            subprocess.run(
                [
                    "huggingface-cli",
                    "download",
                    dataset_name,
                    "--repo-type",
                    "dataset",
                    "--local-dir",
                    str(dataset_path),
                ],
                check=True,
                capture_output=True,
            )
            logger.info(f"Successfully cloned {dataset_name} into {dataset_path}")
            return dataset_path
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to clone repository: {e}")
            raise

    def _build_intermediate_db(self) -> None:
        """
        Create an intermediate DuckDB database to store the datasets.

        Returns:
            None
        """
        con = duckdb.connect(f"{self.data_dir}/intermediate.db", read_only=False)
        con.execute("CREATE TABLE dataset1 (url_host_name VARCHAR, url_count BIGINT)")
        con.execute("CREATE TABLE dataset2 (url_host_name VARCHAR, url_count BIGINT)")

        for dataset in self.dataset1:
            dataset_path = self._pull_dataset(dataset)

            if "CC-MAIN" in dataset or "CC_MAIN" in dataset:
                query = f"SELECT domain AS url_host_name, COUNT(*) AS url_count FROM '{dataset_path}/**/*.parquet' GROUP BY 1"
            else:
                query = f"SELECT url_host_name, url_count FROM '{dataset_path}/data/**/*.parquet'"

            logger.info(f"Running query for dataset {dataset}:\n{query}\n")
            con.execute(f"INSERT INTO dataset1 SELECT * FROM ({query})")
            # Remove the dataset path from the local directory
            shutil.rmtree(dataset_path, ignore_errors=True)
            logger.info(f"Removed dataset path {dataset_path} from local directory")

        for dataset in self.dataset2:
            dataset_path = self._pull_dataset(dataset)

            if "CC-MAIN" in dataset or "CC_MAIN" in dataset:
                query = f"SELECT domain AS url_host_name, COUNT(*) AS url_count FROM '{dataset_path}/**/*.parquet' GROUP BY 1"
            else:
                query = f"SELECT url_host_name, url_count FROM '{dataset_path}/data/**/*.parquet'"

            logger.info(f"Running query for dataset {dataset}:\n{query}\n")
            con.execute(f"INSERT INTO dataset2 SELECT * FROM ({query})")
            # Remove the dataset path from the local directory
            shutil.rmtree(dataset_path, ignore_errors=True)
            logger.info(f"Removed dataset path {dataset_path} from local directory")

        con.close()
        logger.info(f"Created intermediate database at {self.data_dir}/intermediate.db")

    def _calculate_kl_divergence(self) -> float:
        """
        Calculate the Kullback-Leibler divergence between two datasets.

        Returns:
            float: The Kullback-Leibler divergence between the two datasets.
        """
        con = duckdb.connect(f"{self.data_dir}/intermediate.db", read_only=True)

        query = """
        WITH
    P AS (
        SELECT
        url_host_name,
        url_count
        / SUM(url_count) OVER ()            AS p_prob
        FROM dataset1
    ),
    Q AS (
        SELECT
        url_host_name,
        url_count
        / SUM(url_count) OVER ()            AS q_prob
        FROM dataset2
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
        logger.info(f"Running KL divergence query:\n{query}\n")
        result = con.execute(query).fetchone()
        con.close()
        kl_divergence = result[0] if result else 0.0
        return kl_divergence

    def calculate_dataset_divergence(self) -> float:
        """
        Compare two datasets and calculate the Kullback-Leibler divergence.

        Returns:
            float: The Kullback-Leibler divergence between the two datasets.
        """
        # Pull the datasets
        self._build_intermediate_db()
        # Calculate the KL divergence
        kl_divergence = self._calculate_kl_divergence()
        logger.info(f"KL divergence: {kl_divergence}")

        return kl_divergence
