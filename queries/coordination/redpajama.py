from pathlib import Path

from file_handling import crawl

urlpath = Path("./data/redpajama_urls.txt")
querypath = Path("./queries/domains/redpajama_v2.sql")
sub_name = "redpajama"
data_path = Path(f"data/{sub_name}")

data_path.mkdir(parents=True, exist_ok=True)
to_crawl_path = data_path / "to_crawl.txt"
seen_path = data_path / "seen.txt"
error_path = data_path / "error.txt"

with open(urlpath, "r") as file:
    urls = file.readlines()
    urls_filtered = [i for i in urls if "en_" in i]

with open(to_crawl_path, "w") as f:
    for url in urls_filtered:
        f.write(f"{url}")

with open(seen_path, "w") as f:
    pass

with open(error_path, "w") as f:
    pass

crawl(data_path, querypath, crawl_errors=False, is_hf=False, stretch_format=True)
