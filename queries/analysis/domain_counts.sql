COPY (
    SELECT 
        split_part(url, '/', 3) as domain, 
        COUNT(*) as freq 
    FROM read_parquet('hf://datasets/nhagar/c4_en_urls/data/*.parquet') 
    GROUP BY domain 
    ORDER BY freq DESC
) 
TO 'c4_en_urls_domains.csv';
