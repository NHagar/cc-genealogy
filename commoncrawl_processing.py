import os
import time

import boto3
from datasets import Dataset
from dotenv import load_dotenv

load_dotenv()


class AthenaToHuggingFace:
    def __init__(self, database, s3_output_location, region_name="us-east-1"):
        """
        Initialize the Athena client and set up basic configurations.

        Args:
            database (str): The Athena database name
            s3_output_location (str): S3 location for Athena query results
            region_name (str): AWS region name
        """

        # Get AWS credentials
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials not found in environment variables")

        # Initialize AWS clients with explicit credentials
        self.athena_client = boto3.client(
            "athena",
            region_name=region_name,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )

        self.s3_client = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )

        self.database = database
        self.s3_output_location = s3_output_location

    def execute_query(self, query):
        """
        Execute an Athena query and wait for completion.

        Args:
            query (str): The SQL query to execute

        Returns:
            str: Query execution ID
        """
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.s3_output_location},
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
                error_info = response["QueryExecution"]["Status"].get(
                    "StateChangeReason", "No error message provided"
                )
                raise Exception(
                    f"Query failed with state: {state}. Error: {error_info}"
                )

            return query_execution_id

        except Exception as e:
            print(f"Error executing query: {str(e)}")
            raise

    def download_results(self, query_execution_id, local_path):
        """
        Download query results from S3 to local path.

        Args:
            query_execution_id (str): Athena query execution ID
            local_path (str): Local path to save the results
        """
        try:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )

            s3_path = response["QueryExecution"]["ResultConfiguration"][
                "OutputLocation"
            ]
            bucket = s3_path.split("/")[2]
            key = "/".join(s3_path.split("/")[3:])

            self.s3_client.download_file(bucket, key, local_path)

        except Exception as e:
            print(f"Error downloading results: {str(e)}")
            raise

    def process_partitions(self, base_query, partitions, output_dir, token):
        """
        Process each partition, save as parquet, and upload to Hugging Face.

        Args:
            base_query (str): Base SQL query template with {partition} placeholder
            partitions (list): List of partition values
            output_dir (str): Local directory for temporary files
            hf_repo_id (str): Hugging Face repository ID
            token (str): Hugging Face API token
        """
        os.makedirs(output_dir, exist_ok=True)
        for partition in partitions:
            try:
                print(f"Processing partition: {partition}")

                # Generate partition-specific query
                query = base_query.format(partition=partition)

                # Execute query
                print("Executing Athena query...")
                query_id = self.execute_query(query)

                # Download results
                print("Downloading results...")
                local_path = f"{output_dir}/{partition}.parquet"
                self.download_results(query_id, local_path)

                # Upload to Hugging Face
                print("Uploading to Hugging Face...")
                dataset = Dataset.from_parquet(local_path)
                dataset.push_to_hub(repo_id=f"nhagar/CC-{partition}_urls", token=token)
                print(f"Uploaded {partition} to Hugging Face")

                # Clean up local file
                print("Cleaning up local file...")
                os.remove(local_path)

                print(f"Successfully processed partition: {partition}")

            except Exception as e:
                print(f"Error processing partition {partition}: {str(e)}")
                # Continue with next partition even if current one fails
                continue


# Example usage
if __name__ == "__main__":
    # Configuration
    DATABASE = "ccindex"
    S3_OUTPUT_LOCATION = "s3://your-bucket/athena-output/"
    HF_TOKEN = os.getenv("HF_TOKEN_WRITE")
    OUTPUT_DIR = "data/commoncrawl"

    # Initialize processor
    processor = AthenaToHuggingFace(DATABASE, S3_OUTPUT_LOCATION)

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

    partitions = ["2024-01", "2024-02", "2024-03"]

    # Process all partitions
    processor.process_partitions(
        base_query=base_query,
        partitions=partitions,
        output_dir=OUTPUT_DIR,
        token=HF_TOKEN,
    )
