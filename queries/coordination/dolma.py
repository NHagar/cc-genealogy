from pathlib import Path

from queries.crawler import crawl

urlpath = Path("./data/dolma_urls.txt")
querypath = Path("./queries/domains/dolma.sql")
sub_name = "dolma"
data_path = Path(f"data/{sub_name}")

data_path.mkdir(parents=True, exist_ok=True)
to_crawl_path = data_path / "to_crawl.txt"
seen_path = data_path / "seen.txt"
error_path = data_path / "error.txt"

with open(urlpath, "r") as file:
    urls = file.readlines()
    urls_filtered = [
        i
        for i in urls
        if i.split("1_7/")[1].split("/")[0]
        in [
            "c4-filtered",
            "cc_en_head",
            "cc_en_middle",
            "cc_en_tail",
            "falcon-refinedweb-filtered",
        ]
    ]

if not to_crawl_path.exists():
    with open(to_crawl_path, "w") as f:
        for url in urls_filtered:
            f.write(f"{url}")

    with open(seen_path, "w") as f:
        pass

    with open(error_path, "w") as f:
        pass

crawl(data_path, querypath, crawl_errors=True, is_hf=False)
