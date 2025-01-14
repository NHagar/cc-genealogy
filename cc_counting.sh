#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=12:00:00
#SBATCH --mem-per-cpu=16G
#SBATCH --job-name=cc-count

module purge

module load mamba
source ~/.bashrc
mamba activate /home/nrh146/.conda/envs/cc

python ./cc_counting.py