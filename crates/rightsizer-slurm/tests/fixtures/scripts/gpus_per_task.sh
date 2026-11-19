#!/bin/bash
#SBATCH --ntasks=4
#SBATCH --gpus-per-task=1
#SBATCH --mem=128G
srun python worker.py
