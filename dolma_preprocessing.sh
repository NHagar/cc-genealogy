cd /scratch/nrh146

git clone https://huggingface.co/datasets/allenai/dolma cache-dolma

rm -f cache-dolma/urls/*-sample*.txt

# Process each txt file
for file in cache-dolma/urls/*.txt; do
    # Extract filename without extension
    base_name=$(basename "$file" .txt)
    echo "Processing $base_name"
    
    # Create output directory
    mkdir -p "/projects/p32491/cc-genealogy/data/allenai_dolma_${base_name}"
    
    # Filter lines matching patterns and chunk into batches of 100
    grep -E '/c4|/cc_|/falcon-refinedweb' "$file" | \
    awk '{
        batch = int((NR-1) / 100) + 1;
        output_file = "/projects/p32491/cc-genealogy/data/allenai_dolma_" line_base_name "/download_urls_batch_" batch ".txt";
        print $0 > output_file;
    }' line_base_name="$base_name"
    
    echo "Finished processing $base_name into batches"
done

echo "All files processed."