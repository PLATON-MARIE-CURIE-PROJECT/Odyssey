#!/bin/bash
#SBATCH --job-name dress
#SBATCH --ntasks-per-node=1
#SBATCH --exclusive
# SBATCH --partition allprio
echo "Jobid $SLURM_JOB_ID"

echo "Running on: $SLURM_NODELIST"
echo "SLURM_NTASKS=$SLURM_NTASKS"
echo "SLURM_NTASKS_PER_NODE=$SLURM_NTASKS_PER_NODE"
echo "SLURM_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK"
echo "SLURM_NNODES=$SLURM_NNODES"
echo "SLURM_CPUS_ON_NODE=$SLURM_CPUS_ON_NODE"

module purge
module load intel/21U2/suite
module list

mpirun ../bin/ads --simple-work-stealing $DRESS_WORK_STEALING --all-nodes-index-all-dataset no --output-file $DRESS_RESULT_FILE --verbose no --share-bsf $DRESS_QUERY_TYPE --mpi-already-splitted-dataset no --dataset $DRESS_DATASET --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size $DRESS_DATASET_SIZE --timeseries-size $DRESS_TIMESERIES_SIZE --function-type 9990 --in-memory --queries $DRESS_QUERIES --queries-size $DRESS_QUERIES_SIZE --read-block 20000 --flush-limit 1000000 --index-workers $DRESS_INDEX_THREADS --query-workers $DRESS_QUERY_THREADS --parallel-query-workers $DRESS_PARALLEL_QUERY_THREADS --query-mode $DRESS_QUERY_MODE --part-to-load $DRESS_CHUNK_TO_LOAD --distributed-queries-initial-burst $DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST --benchmark_estimations_filename $DRESS_ESTIMATIONS_FILENAME --basis-function-filename $DRESS_BASIS_FUNCTION_FILENAME --th-div-factor $DRESS_TH_DIV_FACTOR --workstealing $DRESS_CH_WORKSTEALING --batches-to-send $DRESS_BATCHES_TO_SEND --share-bsf-chatzakis $DRESS_BSF_CHATZAKIS_SHARING --node-groups $DRESS_NODE_GROUPS --preprocessing $DRESS_PREPROCESS
