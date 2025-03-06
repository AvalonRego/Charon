#!/bin/bash -l
#
# Python MPI4PY example job script for MPCDF Raven.
# May use more than one node.
#
#SBATCH -o ./jobs/TD.%j.out
#SBATCH -e ./jobs/TD.%j.err
#SBATCH -D ./
#SBATCH -J TriggerData
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=20:00:00
#SBATCH --mem=240000
#SBATCH --cpus-per-task=8
#SBATCH --mail-type=all
#SBATCH --mail-user=arego@mpcdf.mpg.de

module purge
module load anaconda/3/2023.03
module load gcc/13


source /viper/u/arego/Project/olympus/bin/activate

# Run the Python script
srun python3 /u/arego/Project/Thesis/triggerdata.py"${@}"

echo "job finished"