#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=8:00:00
#SBATCH --mem-per-cpu=24G
#SBATCH --job-name=domain_data_consolidator

module purge

module load mamba
source ~/.bashrc
mamba activate /home/nrh146/.conda/envs/cc

python queries/consolidate_domain_data.py --database data/allenai_c4_en/database.db
python queries/consolidate_domain_data.py --database data/allenai_MADLAD-400_data-v1p5_*/database.db
python queries/consolidate_domain_data.py --database data/dolma/database.db
python queries/consolidate_domain_data.py --database data/HuggingFaceFW_fineweb-edu_data_*/database.db
python queries/consolidate_domain_data.py --database data/HuggingFaceFW_fineweb_data_*/database.db
python queries/consolidate_domain_data.py --database data/mlfoundations_dclm-baseline-1.0-parquet_filtered_**/database.db
python queries/consolidate_domain_data.py --database data/tiiuae_falcon-refinedweb_data/database.db
python queries/consolidate_domain_data.py --database data/uonlp_CulturaX_*/database.db
python queries/consolidate_domain_data.py --database data/Zyphra_Zyda-2_data_**/database.db
