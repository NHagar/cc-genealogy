from pathlib import Path
import duckdb

from tqdm import tqdm

urlpath = Path("./data/redpajama_urls.txt")
querypath = Path("./queries/domains/redpajama_v2.sql")

con = duckdb.connect("./data/redpajama.db", read_only=False)

with open(urlpath, 'r') as file:
    urls = file.readlines()[:5]

with open(querypath, "r") as file:
    query = file.read()

for i, url in tqdm(enumerate(urls)):
    query_formatted = query.format(url=url.strip())
    create_statement = f"CREATE TABLE redpajama_domains_{i} AS ({query_formatted});"
    con.execute(create_statement)

con.close()
