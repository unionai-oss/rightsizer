#!/bin/bash
#SBATCH --job-name="quoted name"
#SBATCH --mem=12G  # inline comment --mem=1G
#SBATCH --comment='a # inside quotes'
## double-hash comment
#SBATCH --tmp=20G
#SLURM --cpus-per-task=3
python job.py
