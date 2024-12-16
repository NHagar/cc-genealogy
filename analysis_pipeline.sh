# !/bin/bash
# Top-level count
duckdb < queries/analysis/top_level_count.sql
# Domain counts
duckdb < queries/analysis/domain_counts.sql
# Stratified sample
duckdb < queries/analysis/stratified_sample.sql
# Case study search
duckdb < queries/analysis/case_studies.sql

exit 0