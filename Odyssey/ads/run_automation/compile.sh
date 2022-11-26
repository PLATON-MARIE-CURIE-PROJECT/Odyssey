echo "Compiling Shell Script Starts..."
module load python/3.9.5
module load intel/21U2/suite
cd ..
./configure
cd run_automation
cd ..
python3 cleanCompile.py
cd run_automation
echo "Compiling Shell Script Ends..."
