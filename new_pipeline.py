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


ds = load_dataset("allenai/c4", "multilingual", split="train", streaming=True)
ds = ds.remove_columns(["text", "timestamp"])
ds = ds.map(process_data, batched=True, batch_size=50_000)

api = HfApi()
repo_id = "nhagar/c4_urls_multilingual"
api.create_repo(repo_id, exist_ok=True, repo_type="dataset")

# Start timing
start_time = time.time()
print("Starting pipeline...")

chunk_size = 250_000
milestone = 1_000_000  # Renamed from max_items to milestone for clarity
total_processed = 0
chunks_created = 0
milestone_reached = False

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

    # Log milestone when we reach 1M items
    if not milestone_reached and total_processed >= milestone:
        milestone_time = time.time()
        milestone_duration = milestone_time - start_time
        print(f"\n===== Milestone: {milestone:,} items processed =====")
        print(
            f"Time to process first {milestone:,} items: {milestone_duration:.2f} seconds ({milestone_duration / 60:.2f} minutes)"
        )
        print(
            f"Processing speed for first {milestone:,} items: {milestone / milestone_duration:.2f} items per second"
        )
        milestone_reached = True

end_time = time.time()
total_time = end_time - start_time

print("\n===== Pipeline Timing Summary =====")
print(f"Total items processed: {total_processed:,}")
print(f"Number of chunks created: {chunks_created}")
print(
    f"Total processing time: {total_time:.2f} seconds ({total_time / 60:.2f} minutes)"
)
print(f"Average processing speed: {total_processed / total_time:.2f} items per second")
