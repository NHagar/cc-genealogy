#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=2
#SBATCH --time=1:00:00
#SBATCH --mem-per-cpu=8G
#SBATCH --job-name=export

module purge

module load mamba
source ~/.bashrc
mamba activate /home/nrh146/.conda/envs/cc

python -u ./queries/condense_and_upload.py --dataset c4_en
