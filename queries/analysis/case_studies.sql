COPY (
    SELECT 
        url 
    FROM 
        read_parquet('DATASET_PATTERN') 
    WHERE 
        CONTAINS(url, 'nytimes.com') LIMIT 5) 
TO 'OUTPUT_PATH';