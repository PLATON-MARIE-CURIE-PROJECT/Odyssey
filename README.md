# Odyssey 
Repository for the paper "Odyssey: A Journey in the Land of Distributed Data Series Similarity Search", Manos Chatzakis, Panagiota Fatourou, Eleftherios Kosmas, Themis Palpanas, Botao Peng. Revisions, VLDB 2022

* [Compilation](#compilation)
* [Documentation](#documentation)
* [Data Generators](#data-generators)
* [Datasets](#datasets)
* [Experiments](#experiments)
* [Involved People](#involved-people)
* [About](#about)


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


## Documentation
Odyssey consists of a set of modules that the user selects to use to create the index and start answering queries.

### Index Creation Primitives
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

### Data Series Generator
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
We contucted our experimental evaluation with Odyssey algorithms using automated python scripts. Those scripts are configured to schedule specific types of experiments selecting and evaluating different modules of the code.
```sh
cd ads/run_automation/
python3 scheduler.py # after configuration
```
The results of the runs made by scheduler.py are saved in folders with strict hierarchy, that is later exploited by statistics.py script to produce the experimental results of the different runs.
```sh
cd ads/run_automation/
python3 statistics.py # after configuration
```

## Involved People
* [Manos Chatzakis](https://github.com/MChatzakis), University of Crete and ICS-FORTH (chatzakis@ics.forth.gr), Currently at [EPFL](https://www.epfl.ch/en/).
* [Panagiota Fatourou](http://users.ics.forth.gr/~faturu/), University of Crete, ICS-FORTH, University of Paris and LIPADE (faturu@ics.forth.gr)
* Eleftherios Kosmas, University of Crete (ekosmas@csd.uoc.gr)
* [Themis Palpanas](https://helios2.mi.parisdescartes.fr/~themisp/), University of Paris and LIPADE (themis@mi.parisdescartes.fr)
* Botao Peng, Chinese Academy of Sciences (pengbotao@ict.ac.cn)

## About
Odyssey is developed under [Platon](http://users.ics.forth.gr/~faturu/platon/index.html) research project.



