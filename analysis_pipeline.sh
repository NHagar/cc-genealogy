#!/bin/bash
#SBATCH --account=p32491
#SBATCH --partition=short
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=4:00:00
#SBATCH --mem-per-cpu=8G
#SBATCH --job-name=url-analysis

# Check if both arguments are provided
if [ $# -ne 2 ]; then
    echo "Error: Please provide two arguments:"
    echo "1. dataset name"
    echo "2. analysis step"
    exit 1
fi

# Store arguments
dataset=$1
analysis_step=$2

echo "Processing dataset: $dataset"
echo "Running analysis step: $analysis_step"