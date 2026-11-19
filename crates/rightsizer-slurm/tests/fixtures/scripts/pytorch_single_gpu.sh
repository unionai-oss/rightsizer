#!/bin/bash
#SBATCH --job-name=resnet50-train
#SBATCH --partition=gpu
#SBATCH --gres=gpu:a100:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --output=logs/%x-%j.out   # stdout
#SBATCH --error=logs/%x-%j.err

module purge
module load cuda/12.4 python/3.11
source ~/venvs/torch/bin/activate

python train.py --arch resnet50 --epochs 90 --batch-size 256
