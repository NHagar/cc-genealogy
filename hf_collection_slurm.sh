#!/bin/bash
#SBATCH --account=p32491  ## YOUR ACCOUNT pXXXX or bXXXX
#SBATCH --partition=normal  ### PARTITION (buyin, short, normal, etc)
#SBATCH --nodes=1 ## how many computers do you need
#SBATCH --ntasks-per-node=1 ## how many cpus or processors do you need on each computer
#SBATCH --time=12:00:00 ## how long does this need to run (remember different partitions have restrictions on this parameter)
#SBATCH --mem-per-cpu=16G ## how much RAM do you need per node (this effects your FairShare score so be careful to not ask for more than you need))
#SBATCH --job-name=url_collection  ## When you run squeue -u NETID this is how you can identify the job

module purge all

uv run new_pipeline.py