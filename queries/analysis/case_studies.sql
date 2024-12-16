COPY (
    SELECT 
        url 
    FROM 
        read_parquet('hf://datasets/nhagar/c4_en_urls/data/*.parquet') 
    WHERE 
        CONTAINS(url, 'nytimes.com') LIMIT 5) 
TO 'c4_en_urls_casestudies.csv';