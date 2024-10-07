duckdb -csv < queries/domains/c4_en.sql >> data/domain_counts/c4_en.csv
duckdb -csv < queries/domains/c4_en_noblocklist.sql >> data/domain_counts/c4_en_noblocklist.csv
duckdb -csv < queries/domains/c4_multilingual.sql >> data/domain_counts/c4_multilingual.csv
duckdb -csv < queries/domains/c4_realnewslike.sql >> data/domain_counts/c4_realnewslike.csv
duckdb -csv < queries/domains/culturax.sql >> data/domain_counts/culturax.csv
duckdb -csv < queries/domains/falcon_refinedweb.sql >> data/domain_counts/falcon_refinedweb.csv
duckdb -csv < queries/domains/fineweb.sql >> data/domain_counts/fineweb.csv
duckdb -csv < queries/domains/fineweb_edu.sql >> data/domain_counts/fineweb_edu.csv
duckdb -csv < queries/domains/madlad_400_cleaned.sql >> data/domain_counts/madlad_400_cleaned.csv
duckdb -csv < queries/domains/madlad_400_raw.sql >> data/domain_counts/madlad_400_raw.csv

python queries/coordination/dolma.py
python queries/coordination/redpajama.py
