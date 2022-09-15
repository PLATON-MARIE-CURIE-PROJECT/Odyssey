module purge
module load intel/21U2/suite
module load openmpi/gcc-10.3.0/4.1.1
module list

export MKL_DEBUG_CPU_TYPE=5
export MKL_ENABLE_INSTRUCTIONS=AVX2

cd ..

make

cd chatzakis_stuff