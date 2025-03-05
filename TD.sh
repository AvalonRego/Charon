#!/bin/bash -l
#
# Python MPI4PY example job script for MPCDF Raven.
# May use more than one node.
#
#SBATCH -o ./jobs/TT1.%j.out
#SBATCH -e ./jobs/TT1.%j.err
#SBATCH -D ./
#SBATCH -J TT
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=20:00:00
#SBATCH --mem=120000
#SBATCH --cpus-per-task=16
#SBATCH --mail-type=all
#SBATCH --mail-user=arego@mpcdf.mpg.de

module purge
module load anaconda/3/2023.03
module load cuda/12.1
module load jax/0.4.13 
module load cudnn/8.9.0
module load gcc/11

source /raven/u/arego/olympus/bin/activate

# Run the Python script
srun python3 /u/arego/project/Stuff_For_Thesis/triggerdata.py"${@}"

echo "job finished"