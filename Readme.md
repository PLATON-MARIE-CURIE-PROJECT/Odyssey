# Odyssey 
Repository for the paper "Odyssey: A Journey in the Land of Distributed Data Series Similarity Search", Manos Chatzakis, Panagiota Fatourou, Eleftherios Kosmas, Themis Palpanas, Botao Peng. Under review, VLDB 2022

## Roadmap
* [General]()
* [How to run]()
* [Compilation]()
* [Documentation]()
* [Data Generators]()
* [Datasets]()
* [Experiments]()
* [Involved People]()
* [About]()

## General
Odyssey is a modular distributed similarity search framework, appropriate for distributed query answering for big data series collections. It is implemented in C, using MPI. 

## How to run
### Packages
Odyssey is a distributed framework, running over MPI Library for Linux* OS, Version 2021.2.

### Environment
Odyssey is developed, configured and run under a cluster of 16 SR645 nodes, connected through an HDR 100 Infiniband network. Each
node has 128 cores (with no hyper-threading support), 256GB RAM, and runs Red Hat Enterprise Linux release 8.2. 

The cluster supports SLURM scripting, thus the rest of the documentation is based on SLURM scripting utilization.


## Compilation
To compile the program from scratch, use
```sh
cd ads/

mkdir results
mkdir perf

chmod u+x configure
./configure

make
```

Or easier,
```sh
cd ads/run_automation/
chmod u+x compile.sh
./compile.sh
```

### Parameters
The main parameters of Odyssey are listed below:
```sh
mpirun ../bin/ads   --dataset [<string>]
                    --dataset-size [<int>]
                    --function-type [<int> (default 9990)]
                    --in-memory
                    --queries [<string>]
                    --queries-size [<int>]
                    --timeseries-size [<int>]
                    --index-workers [<int>]
                    --query-workers [<int>]
                    --query-mode 
                    --basis-function-filename [<string>]
                    --th-div-factor [<int>]
                    --workstealing [<int> (0 or 1)]
                    --node-groups [<int>]
                    --preprocess [<int> (0 or 1)]
```

### SLURM Example
The program input is the system is given using SLURM scripting. An example slurm script is presented below:
```sh
#!/bin/bash
#SBATCH --job-name pdr
#SBATCH --output pdr-%j.out
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=2
#SBATCH --exclusive
#SBATCH --mem=240000MB

module purge
module load intel/21U2/suite
module list

mpirun ../bin/ads   --dataset /gpfs/scratch/chatzakis/data_size100M_seismic_len256_znorm.bin --dataset-size 100000000 
                    --function-type 9990 --in-memory --queries /gpfs/scratch/chatzakis/benchmark2_seismic_len256_znorm.bin --queries-size 2 
                    --timeseries-size 256 --index-workers 16 --query-workers 64 --query-mode 22 
                    --basis-function-filename /gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt 
                    --th-div-factor 16 --workstealing 0 --node-groups 1
```

### Cloning from Github to S-CAPAD
To clone the project, git ssh should be used (http cloning is not working). To clone to S-CAPAD, you need to configure the known machines from your github profile

### Configuring MPI known hosts
To configure and use MPI for a cluster of machines you must configure the known hosts (https://www.open-mpi.org/faq/?category=rsh)
```sh
ssh-keygen -t rsa
cd $HOME/.ssh
cp id_rsa.pub authorized_keys
eval 'ssh-agent'
ssh-add $HOME/.ssh/id_rsa
```
Do not change the file location and dont use passphrase.
Then, you can run MPI programs.
You can use the id_rsa.pub file key to create ssh keys on github and clone your projects.

### Installing je-malloc
You may want to use Odyssey with custom malloc libraries for better performance. An available library is jemalloc:
```sh
wget https://github.com/jemalloc/jemalloc/releases/download/5.1.0/jemalloc-5.1.0.tar.bz2
tar xvjf jemalloc-5.1.0.tar.bz2 #you may need to do this locally and send the result to the cluster
cd jemalloc-5.1.0
./configure --prefix=path_to_local_dir
make
make install  #this is gonna install jemalloc to path_to_local_dir
```
Then, export jemalloc

```sh
export PATH="/home1/public/chatzakis/jemalloc_install/bin:${PATH}"
```

### Installing automake (autoreconf command)
In some cases, configure or make may need the following command
```sh
autoreconf -f -i
```
In case automake is missing from the machine, you may install it the following way
```sh
wget https://ftp.gnu.org/gnu/automake/automake-1.15.tar.gz
tar -xzvf automake-1.15.tar.gz
./configure  --prefix=/home1/public/chatzakis/automake-1.15_install
export LANGUAGE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
make
mkdir -p /home1/public/chatzakis/
make install
export PATH=/home1/public/chatzakis/automake-1.15_install/bin:$PATH
aclocal --version
```

## Documentation
Odyssey consists of a set of modules that the user selects to use to create the index and start answering queries.

### Index Creation Primitives
#### Data Partitioning
The function below is used for chunk-splitting of the dataset among the available nodes.
```c
/**
 * @brief Splits the data into chunks, each node group gets assigned a chunk and builds the index.
 * 
 * @param ifilename Dataset path
 * @param ts_num Total data seties
 * @param index iSAX index parameter
 * @param parallelism_in_subtree Flag for index creation mode
 * @param node_groups Node groups array
 * @param total_node_groups Total number of node groups
 * @param total_nodes_per_nodegroup Total number of nodes in each group
 * @return float* Raw float array
 */
float *index_creation_node_groups_chatzakis(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree, 
                                            pdr_node_group *node_groups, int total_node_groups, int total_nodes_per_nodegroup);
```

#### Mapping (Density Aware Distribution)
The index preprocessing function below is used to distribute the data to each node before index creation.
```c
/**
 * @brief Density aware distribution of the data
 * 
 * @param ifilename Dataset path
 * @param ts_num Total data seties
 * @param index iSAX index parameter
 * @param parallelism_in_subtree Flag for index creation mode
 * @param node_groups Node groups array
 * @param total_node_groups Total number of node groups
 * @param total_nodes_per_nodegroup Total number of nodes in each group
 * @return float* Raw float array
 */
float *index_creation_node_groups_chatzakis_botao_prepro(const char *ifilename, long int ts_num, isax_index *index, 
                                                         const char parallelism_in_subtree, pdr_node_group *node_groups, 
                                                         int total_node_groups, int total_nodes_per_nodegroup);
```

### Coordination Primitives
#### Query Scheduling
Static Scheduling
```c
/**
 * @brief Static query scheduling
 * 
 * @param ifilename Query file path
 * @param q_num Total queries
 * @param index Data series index
 * @param minimum_distance The minimum starting distance (usually set to INF)
 * @param basis_function_filename Priority queue size prediction parameters (only when workstealing is used)
 * @param search_function Exact search function
 * @param ws_search_function Workstealing exact search function (only when workstealing is used)
 * @return float* An array holding the answer for each query
 */
float *isax_query_answering_pdr_baseline_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, 
                                                   char *basis_function_filename,
                                                   query_result (*search_function)(search_function_params args),
                                                   query_result (*ws_search_function)(ws_search_function_params ws_args));
```

Static Query-Prediction Scheduling (Greedy)
```c
/**
 * @brief Dynamic Query Scheduling
 * 
 * @param ifilename Query file path
 * @param q_num Total queries
 * @param index Data series index
 * @param minimum_distance The minimum starting distance (usually set to INF)
 * @param sort Sort on not the queries (only for predictions)
 * @param estimations_filename File Containing the prediction for each query (only for predictions)
 * @param basis_function_filename Priority queue size prediction parameters (only when workstealing is used)
 * @param field_to_cmp Sort the queries based on initial BSF or estimation file
 * @param search_function Exact search function
 * @param approximate_search_func Approximate search function (only for predictions and initial-bsf mode)
 * @param ws_search_function Workstealing exact search function (only when workstealing is used)
 * @return float* An array holding the answer for each query
 */
float *isax_query_answering_pdr_greedy_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, 
                                                 char *estimations_filename, char *basis_function_filename, char sort,
                                                 greedy_alg_comparing_field field_to_cmp,
                                                 query_result (*search_function)(search_function_params args),
                                                 query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index),
                                                 query_result (*ws_search_function)(ws_search_function_params ws_args))

```

Dynamic Scheduling
```c
/**
 * @brief Dynamic Query Scheduling
 * 
 * @param ifilename Query file path
 * @param q_num Total queries
 * @param index Data series index
 * @param minimum_distance The minimum starting distance (usually set to INF)
 * @param sort Sort on not the queries (only for predictions)
 * @param estimations_filename File Containing the prediction for each query (only for predictions)
 * @param basis_function_filename Priority queue size prediction parameters (only when workstealing is used)
 * @param mode Coordinator module mode: Module, Thread or Coordinator
 * @param field_to_cmp Sort the queries based on initial BSF or estimation file
 * @param search_function Exact search function
 * @param approximate_search_func Approximate search function (only for predictions and initial-bsf mode)
 * @param ws_search_function Workstealing exact search function (only when workstealing is used)
 * @return float* An array holding the answer for each query
 */
float *isax_query_answering_pdr_dynamic_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char sort, 
                                                  char *estimations_filename, char *basis_function_filename,
                                                  dynamic_modes mode,
                                                  greedy_alg_comparing_field field_to_cmp,
                                                  query_result (*search_function)(search_function_params args), 
                                                  query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index),
                                                  query_result (*ws_search_function)(ws_search_function_params ws_args));
```



#### Exact Search Routines
The exact search routines use an input argument struct:
```c
/**
 * @brief Arguments needed for exact search routines
 * 
 */
typedef struct search_function_params
{
	// Mandatory
	int query_id;
	ts_type *ts;
	ts_type *paa;
	isax_index *index;
	node_list *nodelist;
	float minimum_distance;

	// Non-Mandatory
	double (*estimation_func)(double);
	communication_module_data *comm_data; // might cause include probs

	float *shared_bsf_results;
} search_function_params;

/**
 * @brief Arguments needed for workstealing routines of Odyssey
 * 
 */
typedef struct ws_search_function_params
{
	// Mandatory
	int query_id;
	ts_type *ts;
	ts_type *paa;
	isax_index *index;
	node_list *nodelist;
	float minimum_distance;
	float bsf;
	int *batch_ids;
	double (*estimation_func)(double);

	communication_module_data *comm_data;

	float *shared_bsf_results;
} ws_search_function_params;
```

The code currently supports the exact search routine of MESSI and the new, workstealing-supporting algorithm of Odyssey.
```c
/**
 * @brief MESSI exact search
 * 
 * @param args Exact search arguments 
 * @return query_result Contains the float result of the query
 */
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas(search_function_params args);

/**
 * @brief Odyssey exact search 
 * 
 * @param args Exact search arguments 
 * @return query_result Contains the float result of the query
 */
query_result exact_search_workstealing_chatzakis(search_function_params args);


/**
 * @brief Odyssey workstealing (helper node) algorithm
 * 
 * @param ws_args Workstealing algorithms
 * @return query_result Contains the float result of the query
 */
query_result exact_search_workstealing_helper_single_batch_chatzakis(ws_search_function_params ws_args);

```

#### Creating Exact Search Routines
To add new exact search routines, you need to define a function with the following prototype
```c
query_result name_of_function(search_function_params args){
    ...
}
```
For the args parameter, you may
* Initialize the mandatory fields, indicated above
* Add any new field needed, as optional field

Then, you can provide the function as a parameter to any query scheduling function presented in the previous section.

#### Query Modes (most important)
| Query Mode  | Description |
| ----------- | ----------- |
| 0           | Classic DRESS (first version)       |
| 21          | Odyssey Baseline (Static) |

## Data Generators
Odyssey contains two generators: A random walk dataset generator (Each data point in the data series is produced as xi+1=N(xi,1), where N(0,1) is a standard normal distribution) and a query generator (Each query is extracted from the dataset and then noise is added).
### Synthetic Dataset Generator
To compile the generator:
```sh
cd dataset/dataset/

#gcc main.c -o generator -lm -lgsl -lgslcblas -lreadline
make
```
Usage Example:
```sh
# Generate 1000 z-normalized data series of length 256:
./generator --size 1000 --length 256 --z-normalize > /path/to/save/file.bin
```
Example/Usage scripts are provided in the corresponding directory.

### Data Series Query Generator
To compile the generator
```sh
cd ads/run_automation/query_analysis/query_generation/
gcc query_generator.c -o query_generator -lm
```

Usage Example:
```sh
# Noise is usually set to 0.0-0.1
./query_generator $DATASET_PATH $QUERIES_PATH $QUERIES_NUMBER $DATASET_LEN $DATASET_SIZE $NOISE 
```
Example/Usage scripts are provided in the corresponding directory.

## Datasets
We are evaluating the performance of Odyssey using the datasets listed below.

* [Seismic](https://drive.google.com/file/d/1XML3uywZdLxChlk_JgglPtVIEEZGs5qD/view): Contains seismic instrument recordings from stations around the world
* [Astro](https://drive.google.com/file/d/1bmajMOgdR-QhQXujVpoByZ9gHpprUncV/view): Represents celestial objects
* [Deep](https://drive.google.com/file/d/1ecvWA8i0ql-cmMI4oL63oOUIYzaWRc4z/view): Contains deep embeddings originating from a convoltional neural network
* [Sift](https://drive.google.com/file/d/1kWoKRMyaW2jLmOyD-xkaopAbp3GfHbJy/view): Contains image (sift) descriptors






## Experiments
We contucted our experimental evaluation with Odyssey algorithms using automated python scripts. Those scripts are configured to schedule specific types of experiments selecting and evaluating different modules of the code. You can run this script by using:
```sh
cd ads/run_automation/
python3 scheduler.py # after you configure the scheduler
```
The results of the runs made by scheduler.py are saved in folders with strict hierarchy, that is later exploited by statistics.py script to produce the experimental results of the different runs.
```sh
cd ads/run_automation/
python3 statistics.py # after you configure the statistics script
```
Using the scripts above you can reproduce the experiments of the paper. For any questions feel free to contact us.


## Involved People
* [Manos Chatzakis](https://github.com/MChatzakis), Student, University of Crete and ICS-FORTH (chatzakis@ics.forth.gr), Currently studying at [EPFL](https://www.epfl.ch/en/).
* [Panagiota Fatourou](http://users.ics.forth.gr/~faturu/), Professor, University of Crete, ICS-FORTH, University of Paris and LIPADE (faturu@ics.forth.gr)
* Eleftherios Kosmas, Postdoctoral Researcher, University of Crete (ekosmas@csd.uoc.gr)
* [Themis Palpanas](https://helios2.mi.parisdescartes.fr/~themisp/), Professor, University of Paris and LIPADE (themis@mi.parisdescartes.fr)
* Botao Peng, Professor, Chinese Academy of Sciences (pengbotao@ict.ac.cn)

## Platon
Odyssey is developed under [Platon](http://users.ics.forth.gr/~faturu/platon/index.html) research project.



