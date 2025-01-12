COPY (
    SELECT 
        url 
    FROM 
        read_parquet('DATASET_PATTERN') 
    WHERE 
        CONTAINS(url, 'nytimes.com')) 
TO 'OUTPUT_PATH';