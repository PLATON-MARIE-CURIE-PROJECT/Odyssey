#!/bin/sh
#SBATCH -J my_job
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=128 # MPI per node
#SBATCH --cpus-per-task=1

# Thread OpenMP per MPI
#SBATCH --ntasks=256

# Total number of MPI
#SBATCH --exclusive
#SBATCH --time=01:00:00

module purge
#module load openmpi/intel-19U5/4.1.1-ucx-1.10.1
module load openmpi/gcc-10.3.0/4.1.1

export MKL_DEBUG_CPU_TYPE=5
export MKL_ENABLE_INSTRUCTIONS=AVX2

echo "Running on: $SLURM_NODELIST"
export TOTAL_NTASKS=$(($SLURM_NNODES*$SLURM_NTASKS_PER_NODE))

mpirun -n $TOTAL_NTASKS ./my_code.exe