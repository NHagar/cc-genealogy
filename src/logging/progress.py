from pathlib import Path

def list_subdirectories(data_dir="data"):
    data_path = Path(data_dir)
    if data_path.exists():
        return [x for x in data_path.iterdir() if x.is_dir()]
    return []

subdirs = list_subdirectories()

output = ""

for subdir in subdirs:
    err_path = subdir / "error.txt"
    seen_path = subdir / "seen.txt"
    to_crawl_path = subdir / "to_crawl.txt"

    with open(err_path, "r") as f:
        errors = f.readlines()

    with open(seen_path, "r") as f:
        seen = f.readlines()

    with open(to_crawl_path, "r") as f:
        to_crawl = f.readlines()

    tocrawl_not_seen = set(to_crawl) - set(seen)
    errors_not_seen = set(errors) - set(seen)

    out_str = f"""

    Dataset: {subdir.stem}
    Total files: {len(to_crawl)}
    Files seen: {len(seen)}
    Files not seen: {len(tocrawl_not_seen)}
    Errors: {len(errors)}
    Errors not seen: {len(errors_not_seen)}


    """

    output += out_str

with open("data/progress.txt", "w") as f:
    f.write(output)
