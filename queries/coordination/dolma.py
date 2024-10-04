from pathlib import Path
import duckdb

from tqdm import tqdm

urlpath = Path("./data/dolma_urls.txt")
querypath = Path("./queries/domains/dolma.sql")

con = duckdb.connect("./data/dolma.db", read_only=False)

with open(urlpath, 'r') as file:
    urls = file.readlines()
    # TODO: Figure out exactly which URLs we want to include
    urls_filtered = [i for i in urls if i.split("1_7/")[1].split("/")[0] == "c4-filtered"]

with open(querypath, "r") as file:
    query = file.read()

for i, url in tqdm(enumerate(urls[:5])):
    query_formatted = query.format(url=url.strip())
    create_statement = f"CREATE TABLE dolma_domains_{i} AS ({query_formatted});"
    con.execute(create_statement)

con.close()
