import time

import tldextract
from datasets import Dataset, load_dataset
from huggingface_hub import HfApi


def get_tld(url):
    extracted = tldextract.extract(url)
    return f"{extracted.domain}.{extracted.suffix}" if url else None


def process_data(examples):
    examples["tld"] = [get_tld(url) for url in examples["url"]]
    return examples


ds = load_dataset("allenai/c4", "en", split="train", streaming=True)
ds = ds.remove_columns(["text", "timestamp"])
ds = ds.map(process_data, batched=True, batch_size=50_000)

api = HfApi()
repo_id = "nhagar/test-upload-c4"
api.create_repo(repo_id, exist_ok=True, repo_type="dataset")

# Start timing
start_time = time.time()
print("Starting pipeline...")

chunk_size = 250_000
max_items = 1_000_000
total_processed = 0
chunks_created = 0

for i, chunk_ds in enumerate(ds.iter(batch_size=chunk_size)):
    chunk_start_time = time.time()
    chunk_regular = Dataset.from_dict(chunk_ds)

    # Update counts
    items_in_chunk = len(chunk_regular)
    total_processed += items_in_chunk
    chunks_created += 1

    chunk_regular.push_to_hub(
        repo_id=repo_id,
        data_dir=f"chunks/chunk_{i}",
    )

    chunk_end_time = time.time()
    print(
        f"Processed chunk {i}: {items_in_chunk} items in {chunk_end_time - chunk_start_time:.2f} seconds"
    )
    print(f"Total processed so far: {total_processed:,} items")

    # Stop after reaching 1 million items
    if total_processed >= max_items:
        print(f"Reached {max_items:,} items limit, stopping")
        break

end_time = time.time()
total_time = end_time - start_time

print("\n===== Pipeline Timing Summary =====")
print(f"Total items processed: {total_processed:,}")
print(f"Number of chunks created: {chunks_created}")
print(
    f"Total processing time: {total_time:.2f} seconds ({total_time / 60:.2f} minutes)"
)
print(f"Average processing speed: {total_processed / total_time:.2f} items per second")
