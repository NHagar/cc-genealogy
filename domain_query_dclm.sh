#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=long
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=168:00:00
#SBATCH --mem-per-cpu=4G
#SBATCH --job-name=cc-coordinator-dclm

module purge

module load mamba
source ~/.bashrc
mamba activate /home/nrh146/.conda/envs/cc

python ./queries/run_queries.py --dataset dclm
