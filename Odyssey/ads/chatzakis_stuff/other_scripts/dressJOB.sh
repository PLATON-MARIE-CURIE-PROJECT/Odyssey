#!/bin/bash
#SBATCH --job-name dress
#SBATCH --output dress-%j.out
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=1
#SBATCH --exclude=ncpu[011,012,028]

# Start of informative/optional lines
echo "---- Starting DRESS ----"
echo "Jobid $SLURM_JOB_ID"
echo "Running on: $SLURM_NODELIST"
echo "SLURM_NTASKS=$SLURM_NTASKS"
echo "SLURM_NTASKS_PER_NODE=$SLURM_NTASKS_PER_NODE"
echo "SLURM_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK"
echo "SLURM_NNODES=$SLURM_NNODES"
echo "SLURM_CPUS_ON_NODE=$SLURM_CPUS_ON_NODE"
echo "SLURM_NTASKS=$SLURM_NTASKS"
# End of informative/optional lines

module purge
module load intel/21U2/suite
module list

# DRESS specific

# for more details about the flags check the flags file (placed in the same folder)
_SIMPLE_WORK_STEALING=no # options: yes/no
_ALL_NODES_INDEX_ALL_GIVEN_DATASET=no # options: yes/no
_VERBOSE=no # options: yes/no
_MPI_ALREADY_SPLITTED_DATASET=no # options: yes/no

_SHARE_BSF=bsf-dont-share # 2 options:  1) bsf-dont-share OR 2) bsf-block-per-query
_OUTPUT_FILE=results/output.txt # path to write the statistics


# MESSI specific
_DATASET=/gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin
_QUERIES_SIZE=10
_QUERIES=/gpfs/users/chatzakis/datasets_local/queries_ctrl100_seismic_len256_znorm.bin

ds_100gb=100000000 # number of data series to process
_DATASET_SIZE=$ds_100gb
_FILE_LABEL="100GB"

_JEMALLOC_PATH=/gpfs/users/chatzakis/devlibs/jemalloc_installation/lib/libjemalloc.so.2

_FUNCTION_TYPE=9990
_CPU_TYPE=32
_FLUSH_LIMIT=1000000
_READ_BLOCK=20000
_LEAF_SIZE=2000
_INITIAL_LBL_SIZE=2000
_MIN_LEAF_SIZE=2000
_TIME_SERIES_SIZE=256

LD_PRELOAD=$_JEMALLOC_PATH

mpirun \
  -x LD_PRELOAD=$_JEMALLOC_PATH \
  ../bin/ads --simple-work-stealing $_SIMPLE_WORK_STEALING \
  --all-nodes-index-all-dataset $_ALL_NODES_INDEX_ALL_GIVEN_DATASET \
  --verbose $_VERBOSE \
  --share-bsf $_SHARE_BSF \
  --output-file $_OUTPUT_FILE \
  --mpi-already-splitted-dataset $_MPI_ALREADY_SPLITTED_DATASET \
  --file-label $_FILE_LABEL \
  --dataset $_DATASET \
  --leaf-size $_LEAF_SIZE \
  --initial-lbl-size $_INITIAL_LBL_SIZE \
  --min-leaf-size $_MIN_LEAF_SIZE \
  --dataset-size $_DATASET_SIZE \
  --flush-limit $_FLUSH_LIMIT \
  --cpu-type $_CPU_TYPE \
  --function-type $_FUNCTION_TYPE \
  --in-memory \
  --queries $_QUERIES \
  --queries-size $_QUERIES_SIZE \
  --read-block $_READ_BLOCK \
  --timeseries-size $_TIME_SERIES_SIZE


#mpirun ../bin/ads

