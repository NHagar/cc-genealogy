from queries.file_handling import crawl, initialize_crawler

data_path = initialize_crawler(
    "hf://datasets/uonlp/CulturaX/*/*.parquet", skip_existing=True, requires_token=True
)
crawl(data_path, "queries/domains/culturax.sql", requires_token=True, crawl_errors=True)
