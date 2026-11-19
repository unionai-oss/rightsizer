#!/bin/bash
#SBATCH --job-name=server-client
#SBATCH --mem=8G -c 4
#SBATCH hetjob
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=4

srun --het-group=0 ./server : --het-group=1 ./client
