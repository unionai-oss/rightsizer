#!/bin/sh
#SBATCH --array=1-100%10
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=4000
#SBATCH --time=30
#SBATCH -o array_%A_%a.out
#RIGHTSIZER key=preprocess-shard

python preprocess.py --shard ${SLURM_ARRAY_TASK_ID}
