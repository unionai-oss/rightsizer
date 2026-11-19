#!/bin/bash
# A minimal job relying on partition defaults.
#SBATCH --time=10
#SBATCH -c 4

echo "running on $(hostname)"
./simulate --steps 1000
