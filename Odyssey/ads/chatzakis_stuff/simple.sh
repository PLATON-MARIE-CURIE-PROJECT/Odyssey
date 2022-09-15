#!/bin/bash
#SBATCH --job-name pdr
#SBATCH --output pdr-%j.out
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=1
#SBATCH --exclusive
#SBATCH --mem=240000MB

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

# --- query estimation statistics --- 
#mpirun ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --verbose no --share-bsf bsf-dont-share --output-file ./result.txt --mpi-already-splitted-dataset no --file-label "test" --dataset /gpfs/scratch/chatzakis/randomDatasets/data_size104857600random100GB_len256_znorm.bin  --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 104857600 --flush-limit 1000000  --function-type 9990 --in-memory --queries /gpfs/users/chatzakis/queries_statistics/query5000.bin --queries-size 500 --read-block 20000 --timeseries-size 256  --index-workers 16 --query-workers 64 --query-mode 0

# --- benchmarks ---
#mpirun ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --share-bsf bsf-dont-share --mpi-already-splitted-dataset no --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/queries_size10K_seismic_len256_znorm.bin --queries-size 10 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 0

# --- subtrees ---
#mpirun ../bin/ads --simple-work-stealing no --share-bsf bsf-dont-share --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --function-type 9990 --in-memory --queries /gpfs/users/chatzakis/queries_statistics/queries_size10K_seismic_len256_znorm.bin --queries-size 500 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 0
#mpirun ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --share-bsf bsf-dont-share --output-file ./result.txt --mpi-already-splitted-dataset no --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 100000000 --flush-limit 1000000  --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/benchmark1001_seismic_len256_znorm.bin --queries-size 1 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 0

# --- pq_analysis ---
#mpirun ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --share-bsf bsf-dont-share --output-file ./result.txt --mpi-already-splitted-dataset no --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 100000000 --flush-limit 1000000  --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/benchmark2_seismic_len256_znorm.bin --queries-size 2 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 19 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 1 --batches-to-send 2

#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/queries_ctrl100_seismic_len256_znorm.bin --queries-size 100 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 5 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 0 --node-groups 2



#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/benchmark101_seismic_len256_znorm.bin --queries-size 101 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 17 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 0 --node-groups 1 --batches-to-send 4 --share-bsf-chatzakis 0


#mpirun ../bin/ads --share-bsf bsf-dont-share --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/queries_size10K_seismic_len256_znorm.bin --queries-size 5000 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 0
#mpirun ../bin/ads --share-bsf bsf-dont-share --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/queries_ctrl100_seismic_len256_znorm.bin --queries-size 100 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 0

#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/benchmark2_seismic_len256_znorm.bin --queries-size 2 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 22 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 1 --node-groups 1


#! Benchmark query predictions (astro,deep)
#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size270M_astronomy_len256_znorm.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 200000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/generated/queries_ctrl5000_astro_len256_znorm.bin --queries-size 1000 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 0
#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size1B_deep1b_len96_znorm.bin --leaf-size 18000 --initial-lbl-size 18000 --min-leaf-size 18000 --dataset-size 400000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/queries_ctrl100_deep1b_len96_znorm.bin --queries-size 100 --read-block 20000 --timeseries-size 96 --index-workers 16 --query-workers 64 --query-mode 0

#! Benchmark pq size predictions (astro,deep,random)
#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size270M_astronomy_len256_znorm.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 200000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/generated/queries_ctrl500_astro_len256_znorm.bin --queries-size 500 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 19
#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size1B_deep1b_len96_znorm.bin --leaf-size 18000 --initial-lbl-size 18000 --min-leaf-size 18000 --dataset-size 400000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/generated/queries_ctrl500_deep_len96_znorm.bin --queries-size 100 --read-block 20000 --timeseries-size 96 --index-workers 16 --query-workers 64 --query-mode 19
#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/randomDatasets/data_size104857600random100GB_len256_znorm.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 100000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/randomDatasets/queries_ctrl1000_random_len256_znorm.bin --queries-size 100 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 19

#! For Botao:
# 1. Run with no replication (equivalent to Manolis code for qa, mode=21 is sequential qa) (0 for manolis, 2 MESSI, 21 new alg)
#mpirun ../bin/ads --preprocessing 1 --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/benchmark101_seismic_len256_znorm.bin --queries-size 101 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 21 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 0 --node-groups 2 --batches-to-send 4 --share-bsf-chatzakis 1

#2. Run with replication rates (seismic, mode={22,24})
#mpirun ../bin/ads --preprocessing 0 --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/benchmark101_seismic_len256_znorm.bin --queries-size 101 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 21 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 0 --node-groups 1 --batches-to-send 4 --share-bsf-chatzakis 1 --benchmark_estimations_filename "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark101.txt"

#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size270M_astronomy_len256_znorm.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 270000000 --flush-limit 1000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/queries_ctrl100_astronomy_len256_znorm.bin --queries-size 100 --read-block 20000 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 21 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/astro_100classic_sigmoid.txt --th-div-factor 1 --workstealing 0 --node-groups 2 --batches-to-send 4 --share-bsf-chatzakis 1 --benchmark_estimations_filename "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_astro_benchmark101.txt"

mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/new-experiments/queries_ctrl1600_seismic_len256_znorm.bin --queries-size 1600 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 0 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 0 --node-groups 1 --batches-to-send 4
#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size270M_astronomy_len256_znorm.bin --dataset-size 230000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/new-experiments/queries_ctrl1600_astro_len256_znorm.bin --queries-size 1600 --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 22 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 0 --node-groups 1 --batches-to-send 4
#mpirun ../bin/ads --dataset /gpfs/scratch/chatzakis/data_size1B_deep1b_len96_znorm.bin --dataset-size 400000000 --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/new-experiments/queries_ctrl1600_deep_len96_znorm.bin --queries-size 1600 --timeseries-size 96 --index-workers 16 --query-workers 64 --query-mode 22 --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt --th-div-factor 16 --workstealing 0 --node-groups 1 --batches-to-send 4
