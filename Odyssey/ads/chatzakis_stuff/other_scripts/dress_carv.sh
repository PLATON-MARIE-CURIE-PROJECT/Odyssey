DRESS_WORK_STEALING=yes
DRESS_SHARE_BSF=bsf-dont-share
DRESS_RESULT_FILE=./result.txt

DRESS_DATASET=/spare/chatzakis/datasets/data_size100M_seismic_len256_znorm.bin
DRESS_DATASET_SIZE=100000000

DRESS_QUERIES=/spare/chatzakis/datasets/queries_ctrl100_seismic_len256_znorm.bin
DRESS_QUERIES_SIZE=100

DRESS_INDEX_WORKERS=16
DRESS_QUERY_WORKERS=32
DRESS_PARALLEL_QUERY_WORKERS=1
DRESS_QUERY_MODE=0

JEMALLOC_PATH=/home1/public/chatzakis/jemalloc_install/lib/libjemalloc.so.2

PROCESS_NUM=2
MACHINE1="sith0-roce"
MACHINE2="sith1-roce"

mpirun \
-H $MACHINE1,$MACHINE2 \
-n $_PROCESS_NUM \
#-x LD_PRELOAD=$JEMALLOC_PATH \
../bin/ads \
--simple-work-stealing $DRESS_WORK_STEALING \
--all-nodes-index-all-dataset no \
--verbose no \
--share-bsf $DRESS_SHARE_BSF \
--output-file $DRESS_RESULT_FILE \
--mpi-already-splitted-dataset no \
--file-label "test" \
--dataset $DRESS_DATASET  \
--leaf-size 2000 \
--initial-lbl-size 2000 \
--min-leaf-size 2000 \
--dataset-size $DRESS_DATASET_SIZE \
--flush-limit 1000000  \
--function-type 9990 \
--in-memory \
--queries $DRESS_QUERIES \
--queries-size $DRESS_QUERIES_SIZE \
--read-block 20000 \
--timeseries-size 256 \
--index-workers $DRESS_INDEX_WORKERS \
--query-workers $DRESS_QUERY_WORKERS \
--parallel-query-workers $DRESS_PARALLEL_QUERY_WORKERS \
--query-mode $DRESS_QUERY_MODE \
