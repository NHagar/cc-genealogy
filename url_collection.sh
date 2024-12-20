#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=1:00:00
#SBATCH --mem-per-cpu=4G
#SBATCH --job-name=url-collection

module purge

module load mamba
source ~/.bashrc
mamba activate /home/nrh146/.conda/envs/cc

python -u ./queries/url_collection.py --target_dataset cultura
