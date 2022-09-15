#!/bin/bash
declare -A datasets
num_columns=4

# num_rows=1
# datasets[1,1]="/home/ekosmas/datasets/dataset10GB.bin"
# datasets[1,2]="10485760"
# datasets[1,3]="/home/ekosmas/datasets/query10.bin"
# datasets[1,4]="10"

num_rows=1
datasets[1,1]="/spare/papadosp/dataset20GB.bin"
datasets[1,2]="20971520"
datasets[1,3]="/spare/papadosp/query10.bin"
datasets[1,4]="10" 

#num_rows=1
#datasets[1,1]="/spare/ekosmas/dataset100GB.bin"
#datasets[1,2]="104857600"
#datasets[1,3]="/home/ekosmas/datasets/query10.bin"
#datasets[1,4]="10"

# num_rows=2
# datasets[1,1]="/home/ekosmas/datasets/dataset10GB.bin"
# datasets[1,2]="10485760"
# datasets[1,3]="/home/ekosmas/datasets/query10.bin"
# datasets[1,4]="10"
# datasets[2,1]="/spare/ekosmas/dataset100GB.bin"
# datasets[2,2]="104857600"
# datasets[2,3]="/home/ekosmas/datasets/query10.bin"
# datasets[2,4]="10"

iterations=1

for ((i=1;i<=num_rows;i++)) 
do
	filename="results/results_[${datasets[$i,2]}].txt"

	echo "Dataset [${datasets[$i,1]}] with size [${datasets[$i,2]}]"
	echo "Dataset [${datasets[$i,2]}]" >> $filename

	# for num_threads in 1 4 8 16 32 48 64 80
	# for num_threads in 64 48 32 16 8 4 1
	for num_threads in 24
	do
		echo "Threads [$num_threads]"
		echo "Threads [$num_threads]" >> $filename

		# for read_block_length in 20000 30000 40000 50000 100 1000 10000
	        for read_block_length in 20000
		do

			echo "Read Block Length [$read_block_length]"
			echo "Read Block Length [$read_block_length]" >> $filename


			# for version in 0 9990 99904
			for version in 9990
			do
				echo "Running version [$version]"
				echo "Running version [$version]" >> $filename

				# for iteration in 1 2 3 4 5 6 7 8 9 10 # number of runs
				# # for iteration in 1 2 # number of runs
				# do
			        # LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` perf stat -a -e cache-references,cache-misses,L1-dcache-load,L1-dcache-loads-misses,L1-dcache-stores,cache-references:u,cache-misses:u,L1-dcache-load:u,L1-dcache-loads-misses:u,L1-dcache-stores:u -r $iterations -o ./perf/perf[${datasets[$i,2]}.$num_threads.$version.$read_block_length].out ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type $num_threads --function-type $version --in-memory --queries ${datasets[$i,3]} --queries-size ${datasets[$i,4]} --cpu-type $num_threads --read-block $read_block_length
			        LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type $num_threads --function-type $version --in-memory --queries ${datasets[$i,3]} --queries-size ${datasets[$i,4]} --read-block $read_block_length
			      
						  # ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type 80 --function-type $version --in-memory
			    # done
			done
		done
	done
done


#  LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` ./bin/ads --dataset /spare/papadosp/dataset20GB.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 20971520 --flush-limit 1000000 --cpu-type 24 --function-type 9990 --in-memory --queries /spare/papadosp/query10.bin --queries-size 10 --read-block 20000


# The same Commands with and without LD_PRELOAD
# without LD_PRELOAD
# ./bin/ads --dataset /spare/papadosp/dataset20GB.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 20971520 --flush-limit 1000000 --cpu-type 24 --function-type 9990 --in-memory --queries /spare/papadosp/query10.bin --queries-size 10 --read-block 20000

# with LD_PRELOAD
# LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` ./bin/ads --dataset /spare/papadosp/dataset20GB.bin --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size 20971520 --flush-limit 1000000 --cpu-type 24 --function-type 9990 --in-memory --queries /spare/papadosp/query10.bin --queries-size 10 --read-block 20000

