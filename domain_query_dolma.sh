#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=48:00:00
#SBATCH --mem-per-cpu=4G
#SBATCH --job-name=cc-coordinator

module purge

module load mamba
source ~/.bashrc
mamba activate /home/nrh146/.conda/envs/cc

python ./queries/coordination/dolma.py
