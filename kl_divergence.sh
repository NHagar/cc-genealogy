#!/bin/bash
#SBATCH --account=p32491  ## YOUR ACCOUNT pXXXX or bXXXX
#SBATCH --partition=normal  ### PARTITION (buyin, short, normal, etc)
#SBATCH --nodes=1 ## how many computers do you need
#SBATCH --ntasks-per-node=4 ## how many cpus or processors do you need on each computer
#SBATCH --time=48:00:00 ## how long does this need to run (remember different partitions have restrictions on this parameter)
#SBATCH --mem-per-cpu=8G ## how much RAM do you need per node (this effects your FairShare score so be careful to not ask for more than you need))
#SBATCH --job-name=kl_divergence  ## When you run squeue -u NETID this is how you can identify the job
#SBATCH --mail-user=nicholas.hagar@northwestern.edu
#SBATCH --mail-type=ALL
#SBATCH --output=logs/kl_divergence_%j.out  # Standard output log
#SBATCH --error=logs/kl_divergence_%j.err   # Standard error log

# Ensure output directory exists
mkdir -p logs

# Print job information for debugging
echo "Job ID: $SLURM_JOB_ID"
echo "Running on host: $(hostname)"
echo "Running on cluster: $(hostname -d)"
echo "Start time: $(date)"
echo "Directory: $(pwd)"

# Ensure Python output is not buffered so logs appear in real-time
export PYTHONUNBUFFERED=1

# Load required modules
module purge all
module load git-lfs

# Run the script with explicit logging level
echo "Starting KL divergence calculation at $(date)"
uv run calculate_dataset_divergence.py --remote --log-level INFO

echo "Job completed at $(date)"