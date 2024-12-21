import os
import time

import boto3
from datasets import load_dataset


class AthenaToHuggingFace:
    def __init__(self, database, region_name="us-east-1"):
        """
        Initialize the Athena client and set up basic configurations.

        Args:
            database (str): The Athena database name
            s3_output_location (str): S3 location for Athena query results
            region_name (str): AWS region name
        """
        self.athena_client = boto3.client("athena", region_name=region_name)
        self.database = database

    def execute_query(self, query):
        """
        Execute an Athena query and wait for completion.

        Args:
            query (str): The SQL query to execute

        Returns:
            str: Query execution ID
        """
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
        )

        query_execution_id = response["QueryExecutionId"]

        # Wait for query to complete
        while True:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = response["QueryExecution"]["Status"]["State"]

            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            time.sleep(5)

        if state != "SUCCEEDED":
            raise Exception(f"Query failed with state: {state}")

        return query_execution_id

    def get_query_results(self, query_execution_id):
        """
        Get query results directly from Athena.

        Args:
            query_execution_id (str): Athena query execution ID

        Returns:
            list: List of rows with column headers
        """
        paginator = self.athena_client.get_paginator("get_query_results")
        rows = []

        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            for row in page["ResultSet"]["Rows"]:
                rows.append([field.get("VarCharValue", "") for field in row["Data"]])

        return rows

    def download_results(self, query_execution_id, local_path):
        """
        Download query results directly to local path.

        Args:
            query_execution_id (str): Athena query execution ID
            local_path (str): Local path to save the results
        """
        import pandas as pd

        # Get results directly from Athena
        rows = self.get_query_results(query_execution_id)

        # First row contains column headers
        headers = rows[0]
        data = rows[1:]

        # Convert to DataFrame and save as parquet
        df = pd.DataFrame(data, columns=headers)
        df.to_parquet(local_path)

    def process_partitions(self, base_query, partitions, output_dir, token):
        """
        Process each partition, save as parquet, and upload to Hugging Face.

        Args:
            base_query (str): Base SQL query template with {partition} placeholder
            partitions (list): List of partition values
            output_dir (str): Local directory for temporary files
            token (str): Hugging Face API token
        """
        os.makedirs(output_dir, exist_ok=True)

        for partition in partitions:
            # Generate partition-specific query
            query = base_query.format(partition=partition)

            # Execute query
            query_id = self.execute_query(query)

            # Download results
            local_path = f"{output_dir}/{partition}.parquet"
            self.download_results(query_id, local_path)

            # Upload to Hugging Face
            dataset = load_dataset("parquet", data_files=local_path)
            dataset.push_to_hub(f"nhagar/{partition}_urls", token=token)
            print(f"Uploaded {partition} to Hugging Face")

            # Clean up local file
            os.remove(local_path)


# Example usage
if __name__ == "__main__":
    # Configuration
    DATABASE = "ccindex"
    HF_TOKEN = os.getenv("HF_TOKEN_WRITE")
    OUTPUT_DIR = "data/commoncrawl"

    # Initialize processor
    processor = AthenaToHuggingFace(DATABASE)

    # Example query template and partitions
    base_query = """
    SELECT 
        crawl, 
        url_host_name, 
        COUNT(*) AS url_count 
    FROM "ccindex"."ccindex" 
    WHERE 
        crawl = '{partition}' 
    GROUP BY 1, 2
    """

    # TODO: Define from files
    partitions = ["2024-01", "2024-02", "2024-03"]

    # Process all partitions
    processor.process_partitions(
        base_query=base_query,
        partitions=partitions,
        output_dir=OUTPUT_DIR,
        token=HF_TOKEN,
    )
