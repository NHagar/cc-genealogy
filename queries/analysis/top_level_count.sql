COPY (
    SELECT 
        COUNT(*) AS total_count
    FROM 
        read_parquet('hf://datasets/nhagar/c4_en_urls/data/*.parquet')
) 
TO 'c4_en_urls_count.csv';
