#!/bin/bash

machines=("sith1" "sith2" "sith3" "sith4" "sith6")
for machine in ${machines[@]}; do

	ssh $machine '
			echo Before: $GSL_RNG_SEED;
			export GSL_RNG_SEED=8462; # use this command to create identical datesets
			echo After: $GSL_RNG_SEED;  

			###
			dataset_name='dataset_100_and_200GB.bin';
			# rm /spare/papadosp/$dataset_name;

			# ~/Distributed-MESSI_WORKING/dataset/dataset/generator --size 300 --length 256 --z-normalize > /spare/papadosp/$dataset_name; # 300 queries

			# echo After: $GSL_RNG_SEED;
			# ~/Distributed-MESSI_WORKING/dataset/dataset/generator --size 104857600 --length 256 --z-normalize > /spare/papadosp/dataset_100_and_190GB.bin; # 100 GB
			# ~/Distributed-MESSI_WORKING/dataset/dataset/generator --size 199229440 --length 256 --z-normalize > /spare/papadosp/dataset_100_and_190GB.bin; # 190 GB
	' # & # background

done

# 52428800 data series for 50GB 
# 157286400 data series for 150GB dataset


# 53687091200
# 53687091200
# 					+
# ------------
# 107374182400 # 100 GB more





# 53687091200 #50 Gb data series
# 26843545600 #25 gb data series
# 					+
# ------------
# 80530636800 #75GB more


# 53687091200 #50 Gb data series
# 26843545600 #25 gb data series
# 10737418240 #5GB data series
# 					+
# ------------
# 91268055040 #80GB more

# 104333456000 free space


# 104857600 100GB

# 52428800  50GB
# 10485760  10GB
# 10485760  10GB
# 10485760  10GB
# 10485760  10GB
# +
# ----------
# 199229440 DATA_SERIES





# EXTRA
# 94371840 DATA_SERIES

# 96636764160 BYTES
# 104333456000 Availbale Bytes


# All available bytes: 
# 209188860000
# 204010946560

# 167772160 <---- ok (100GB+50GB+10GB)
# 157286400 <---- 150GB
# 209715200












# 0) 
#------------------------------------------------------------
#scp /spare/ekosmas/dataset100GB.bin papadosp@$machine:/spare/papadosp
#ssh $machine 'rm /spare/papadosp/query10.bin'
#scp /spare/papadosp/query10.bin papadosp@$machine:/spare/papadosp

# continue
#------------------------------------------------------------



# 1)
#------------------------------------------------------------
# generate datasets with different name on each machine!
	# ssh $machine '
	# 		echo $GSL_RNG_SEED;
	# 		export GSL_RNG_SEED=100; # use this command to create identical datesets
	# 		echo After: $GSL_RNG_SEED;  

	# 		### find the host name
	# 		full_host_name="$(hostname)";
	# 		arrIN=(${full_host_name//./ }); # split with '.' (sith5.cluster.ics.forth.gr)
	# 		host_name=${arrIN[0]};
	# 		dataset_name="dataset_${host_name}.bin";
			
	# 		echo $host_name;
	# 		echo $dataset_name;

	# 		# rm /spare/papadosp/$dataset_name

	# 		###
	# 		./Distributed-MESSI_WORKING/dataset/dataset/generator --size 102400 --length 256 --z-normalize > /spare/papadosp/$dataset_name # 100 MB
	# ' & # background
#------------------------------------------------------------


# 2) 
#------------------------------------------------------------
### WORKS!!!! 
# comm1='ls;'
# comm2='echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@;'
# comm3='ls -sh;'
# # echo "${comm1} ${comm2}"
# ssh $machine "${comm1} ${comm2} ${comm3}"
#------------------------------------------------------------


# 3)
#------------------------------------------------------------
# variableA=$(rm dataset*)
# status=$?
# echo 'Remove all dataset files (0-yes, 1-no)?' $status
# exit

# random=$RANDOM

# echo 'The random number is:' $random
# echo '------------------------------------'
#------------------------------------------------------------


# 4)
#------------------------------------------------------------
# Execute line by line
#ssh sith8 'sleep 5 && cat /etc/hostname'
#ssh sith1 'sleep 2 && cat /etc/hostname'
#echo "Hello, World!"
#rm text1.txt


# Try to execute ssh commands independent
#ssh sith8 '(nohup sleep 15 && nohup touch sith8_file && nohup logout) &' &
#echo "command 1 complete"
#ssh sith1 '(nohup sleep 4 && nohup touch sith5_file && nohup logout) &' &
#echo "command 2 complete"
## ssh sith1 '((nohup sleep 4 &) && (nohup touch sith5_file & )  && (exit)) &' &"
#echo "Hello from sith5"


#ssh sith8 '(nohup sleep 2 && nohup touch sith8_file && nohup ls > sith8_output  && nohup logout) &' &
#echo "command 1 complete"
#ssh sith1 '(nohup sleep 4 && nohup touch sith5_file && nohup ls > sith5_output  && nohup logout) &' &
#echo "command 2 complete"
## ssh sith1 '((nohup sleep 4 &) && (nohup touch sith5_file & )  && (exit)) &' &"
#echo "Hello from sith5"

# ------------------------------------------
#ssh sith8 'echo $GSL_RNG_SEED'
#ssh sith8 'echo $GSL_RNG_SEED'
#ssh sith8 'echo $GSL_RNG_SEED'
#ssh sith8 'echo $GSL_RNG_SEED'

#source .bashrc
#echo $GSL_RNG_SEED
#source .bashrc
#echo $GSL_RNG_SEED
# -------------------------------------------
#------------------------------------------------------------
