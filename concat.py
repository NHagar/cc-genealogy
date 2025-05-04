from datasets import concatenate_datasets, load_dataset

# Load your datasets
dataset1 = load_dataset("nhagar/zyda-2_urls_dclm_crossdeduped", split="train")
dataset2 = load_dataset("nhagar/zyda-2_urls_zyda_crossdeduped-filtered", split="train")
dataset3 = load_dataset(
    "nhagar/zyda-2_urls_dolma-cc_crossdeduped-filtered", split="train"
)
dataset4 = load_dataset("nhagar/zyda-2_urls_fwe3", split="train")

# Verify they have the same feature structure
# This is important because concatenate_datasets requires matching schemas
print(dataset1.features.type)
print(dataset2.features.type)
print(dataset3.features.type)
print(dataset4.features.type)

# Combine all datasets into one
combined_dataset = concatenate_datasets([dataset1, dataset2, dataset3, dataset4])

# Check the combined dataset size
print(f"Combined dataset contains {len(combined_dataset)} examples")

# Push to the Hub
combined_dataset.push_to_hub("nhagar/zyda-2_urls")
