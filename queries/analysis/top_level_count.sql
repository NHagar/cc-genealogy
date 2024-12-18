COPY (
    SELECT 
        COUNT(*) AS total_count
    FROM 
        read_parquet('DATASET_PATTERN')
) 
TO 'OUTPUT_PATH';
