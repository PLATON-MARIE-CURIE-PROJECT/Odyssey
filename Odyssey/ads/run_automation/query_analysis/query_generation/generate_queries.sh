QUERIES_SIZE=1600
NOISE=0.04

SEISMIC_DATASET=/gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin
SEISMIC_SIZE=100000000
SEISMIC_LEN=256
SEISMIC_QUERIES=/gpfs/scratch/chatzakis/new-experiments/queries_ctrl${QUERIES_SIZE}_seismic_len256_znorm.bin

ASTRO_DATASET=/gpfs/scratch/chatzakis/data_size270M_astronomy_len256_znorm.bin
ASTRO_SIZE=230000000
ASTRO_LEN=256
ASTRO_QUERIES=/gpfs/scratch/chatzakis/new-experiments/queries_ctrl${QUERIES_SIZE}_astro_len256_znorm.bin

DEEP_DATASET=/gpfs/scratch/chatzakis/data_size1B_deep1b_len96_znorm.bin
DEEP_SIZE=400000000
DEEP_LEN=96
DEEP_QUERIES=/gpfs/scratch/chatzakis/new-experiments/queries_ctrl${QUERIES_SIZE}_deep_len96_znorm.bin

SIFT_DATASET=/gpfs/scratch/chatzakis/data_size1B_sift_len128_znorm.bin
SIFT_SIZE=400000000
SIFT_LEN=128
SIFT_QUERIES=/gpfs/scratch/chatzakis/new-experiments/queries_ctrl${QUERIES_SIZE}_sift_len128_znorm.bin

gcc query_generator.c -o query_generator -lm
mkdir /gpfs/scratch/chatzakis/generated/
module load intel/21U2/suite

#./query_generator $SEISMIC_DATASET $SEISMIC_QUERIES $QUERIES_SIZE $SEISMIC_LEN $SEISMIC_SIZE $NOISE 
#echo "Seismic Queries Produced"

./query_generator $ASTRO_DATASET $ASTRO_QUERIES $QUERIES_SIZE $ASTRO_LEN $ASTRO_SIZE $NOISE 
echo "Astro Queries Produced"

#./query_generator $DEEP_DATASET $DEEP_QUERIES $QUERIES_SIZE $DEEP_LEN $DEEP_SIZE $NOISE 
#echo "Deep Queries Produced"

#./query_generator $SIFT_DATASET $SIFT_QUERIES $QUERIES_SIZE $SIFT_LEN $SIFT_SIZE $NOISE 
#echo "Sift Queries Produced"

