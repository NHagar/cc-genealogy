#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=normal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=24:00:00
#SBATCH --mem-per-cpu=4G
#SBATCH --job-name=cc-coordinator

module purge

module load mamba

mamba init

mamba activate /home/nrh146/.conda/envs/cc

python ./queries/run_queries.py --dataset c4_en
