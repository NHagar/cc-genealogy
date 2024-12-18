COPY (
    SELECT 
        split_part(url, '/', 3) as domain, 
        COUNT(*) as freq 
    FROM read_parquet('DATASET_PATTERN') 
    GROUP BY domain 
    ORDER BY freq DESC
) 
TO 'OUTPUT_PATH';
