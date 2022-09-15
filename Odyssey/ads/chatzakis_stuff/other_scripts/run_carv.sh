# DRESS specific

machine1='sith0-roce'
machine2='sith1-roce'
machine3='sith6-roce'
machine4='sith7-roce'
machine5='sith8-roce'
machine6='vader'

_PROCESS_NUM=1

# for more details about the flags check the flags file (placed in the same folder)
_SIMPLE_WORK_STEALING=no # options: yes/no
_ALL_NODES_INDEX_ALL_GIVEN_DATASET=no # options: yes/no
_VERBOSE=no # options: yes/no
_MPI_ALREADY_SPLITTED_DATASET=no # options: yes/no

_SHARE_BSF=bsf-dont-share # 2 options:  1) bsf-dont-share OR 2) bsf-block-per-query
_OUTPUT_FILE=results/22-03/output.txt # path to write the statistics


# MESSI specific
#_DATASET=/spare2/papadosp/real_datasets/seismic-105GB
_DATASET=/spare/ekosmas/dataset100GB.bin

#_QUERIES_SIZE=100
_QUERIES_SIZE=10

#_QUERIES=/spare/ekosmas/dataset100GB.bin
_QUERIES=/home1/public/chatzakis/queries/query10.bin

ds_100gb=104857600 # number of data series to process
#ds_100gb=10485760

_DATASET_SIZE=$ds_100gb
_FILE_LABEL="test"

_JEMALLOC_PATH=/home1/public/chatzakis/jemalloc_install/lib/libjemalloc.so.2

_FUNCTION_TYPE=9990
_CPU_TYPE=32
_FLUSH_LIMIT=1000000
_READ_BLOCK=20000
_LEAF_SIZE=2000
_INITIAL_LBL_SIZE=2000
_MIN_LEAF_SIZE=2000
_TIME_SERIES_SIZE=256


mpirun \
  -H $machine6 \
  -n $_PROCESS_NUM \
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
