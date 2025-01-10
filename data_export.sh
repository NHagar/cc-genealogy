#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=24:00:00
#SBATCH --mem-per-cpu=64G
#SBATCH --job-name=export

module purge

module load mamba
source ~/.bashrc
mamba activate /home/nrh146/.conda/envs/cc

python -u ./queries/upload_to_hf.py --dataset fineweb --scratch --large
