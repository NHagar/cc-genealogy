import os

import duckdb
from dotenv import load_dotenv

load_dotenv()

con = duckdb.connect("./data/domain_counts/culturax.db")

with open("./queries/domains/culturax.sql", "r") as file:
    query = file.read()


HF_TOKEN = os.getenv("HF_TOKEN")
formatted_query = query.format(hf_token_value=HF_TOKEN)
con.execute(formatted_query)

con.close()
