#!/bin/bash
#SBATCH --job-name messibotao
#SBATCH --output messibotao%j.out
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=4
#SBATCH --exclusive
# SBATCH --partition allprio
#SBATCH --mem=100000MB
#SBATCH --exclude=ncpu[011,012,028]
echo "Jobid $SLURM_JOB_ID"


echo "Running on: $SLURM_NODELIST"
echo "SLURM_NTASKS=$SLURM_NTASKS"
echo "SLURM_NTASKS_PER_NODE=$SLURM_NTASKS_PER_NODE"
echo "SLURM_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK"
echo "SLURM_NNODES=$SLURM_NNODES"
echo "SLURM_CPUS_ON_NODE=$SLURM_CPUS_ON_NODE"
echo "SLURM_NTASKS=$SLURM_NTASKS"

module purge
module load intel/21U2/suite
module list
#mpiifort hellompi.f90 -o hellompi.x

echo "Go !!"

#mpirun ./hellompi.x

mpirun ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --output-file ../results/node2/simple/run2.txt --verbose no --share-bsf bsf-block-per-query --mpi-already-splitted-dataset no --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin  --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 100000000 --timeseries-size 256 --cpu-type 8 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/queries_ctrl100_seismic_len256_znorm.bin --queries-size 100 --read-block 20000 --flush-limit 1000000

#mpirun ./code/ads/bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --verbose no --output-file a$

#mpirun ./mycode/ads/bin/ads --help
#mpirun ./messimpi/bin/MESSI --dataset /gpfs/scratch/bpeng/randomdataset.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 10000000 --flush-limit 1000000 --cpu-type 4 --function-type 0 --in-memory --queries /gpfs/scratch/bpeng/output.bin --queries-size 100 --read-block 20000
#mpirun ./mycode/ads/bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --verbose no --output-file abccc.txt --share-bsf bsf-dont-share --mpi-already-splitted-dataset no --dataset /gpfs/scratch/bpeng/randomdataset.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 600000000 --timeseries-size 256 --cpu-type 4 --function-type 9990 --in-memory --queries /gpfs/scratch/bpeng/output.bin --queries-size 100 --read-block 20000 --flush-limit 1000000
