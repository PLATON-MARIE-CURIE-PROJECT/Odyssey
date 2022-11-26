//
//  main.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/12/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
//  Updated by Eleftherios Kosmas on May 2020.
//
#define _GNU_SOURCE

#define PRODUCT "----------------------------------------------\
\nThis is the Adaptive Leaf iSAX index.\n\
Copyright (C) 2011-2014 University of Trento.\n\
----------------------------------------------\n\n"
#ifdef VALUES
#include <values.h>
#endif

#include "../../config.h"
#include "../../globals.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <math.h>
#include <getopt.h>
#include <time.h>
#include <float.h>
#include <sched.h>
#include <string.h>
#include <stdio.h>

#include "ads/sax/sax.h"
#include "ads/sax/ts.h"
#include "ads/isax_visualize_index.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_visualize_index.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/isax_query_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/parallel_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/inmemory_topk_engine.h"

#define CHECK_FOR_CORRECT_VERSION(function_type)                                 \
    if (function_type != 9990)                                                   \
    {                                                                            \
        printf("ERROR: Version [%d] of MESSI does not exist!\n", function_type); \
        exit(0);                                                                 \
    }

#include <mpi.h>

// signatures
long int split_data_series_for_processes(long int dataset_size);

void print_process_bind_info(int my_rank);
void recv_final_result(int queries_size);
void collect_dists_stats();
void collect_bsf_stats();
void collect_queries_times(int queries_size);
void collect_threads_total_difference_stats();
void get_options_for_MESSI(int argc, char **argv);

/* Chatzakis */
void keep_info_to_files_chatzakis(int times_num, int times_nodes, double times[times_nodes][times_num]);
void classic_MESSI_chatzakis();
void initialize_queries_statistics_chatzakis();
void process_results_chatzakis(MPI_Request request);
isax_index *create_query_index_chatzakis();
void initial_log_chatzakis();
void traverse_root_subtree_chatzakis(isax_node *curr_root, long int *);
void traverse_query_tree_buffers_chatzakis(isax_index *index);
void print_query_groups();
void check_queries_distances(isax_index *index);
void create_benchmarks_chatzakis(isax_index *index);
long int split_data_series_for_node_groups(long int total_time_series, long int groups_to_create);
void DRESS_chatzakis(int argc, char **argv);

// Related to MESSI
char *dataset = NULL; //"/spare/papadosp/dataset20GB.bin ";
char *queries = NULL; //"/spare/papadosp/query10.bin";
char *index_path = "/home/botao/document/myexperiment/";
char *labelset = NULL;            //"/home/botao/document/myexperiment/";
long int dataset_size = 20971520; // testbench // number of data series
int queries_size = 10;
int time_series_size = 256;
int paa_segments = 16;
int sax_cardinality = 8;
int leaf_size = 2000;
int min_leaf_size = 2000;
int initial_lbl_size = 2000;
int flush_limit = 1000000;
int initial_fbl_size = 100;
char use_index = 0;
int complete_type = 0;
int total_loaded_leaves = 1;
int tight_bound = 0;
int aggressive_check = 0;
float minimum_distance = FLT_MAX;
int serial_scan = 0;
char knnlabel = 0;
int min_checked_leaves = -1;
int cpu_control_type = 24;
char inmemory_flag = 1;

int function_type = 9990;
int k_size = 0;
long int labelsize = 1;
int topk = 0;
int calculate_thread = 8;

int mpi_already_splitted_dataset = 0;      //-1; // is the dataset already split ?
int share_with_nodes_bsf = BSF_DONT_SHARE; // share the bsf with other nodes
int simple_work_stealing = 0;              // for the work-stealing technique

char *file_label = "without-label";        // does not used
char *output_file = "./results.txt";       // where to write the times etc.
int all_nodes_index_all_given_dataset = 0; // ingnore this technique, each node build the whole dataset and then answer a query on the half of the subtrees (if there is two nodes)

// variables
int my_rank; // we use it to inmemory_index_engine.c. extern int my_rank
int comm_sz; // >>     >>

MPI_Comm *communicators = NULL;     // we need the communicators because each node sends the BSF value to the other node
MPI_Request *requests = NULL;       // one for each communicator, to check if there is a message arrived
struct bsf_msg *shared_bsfs = NULL; // one for each node, represent the received value from each

// keep the index trees
isax_index *idx_1 = NULL; // create tree index for the node data
isax_index *idx_2 = NULL; // create tree index for the other node (workstealing)

// pointer to raw file (data series in memory)
float *raw_file_1 = NULL;
float *raw_file_2 = NULL;

// each time points to one raw_file_1 or raw_file_2
float *tmp_raw_file = NULL;

// statistics
extern unsigned long long int for_all_queries_real_dist_counter; // node specific
extern unsigned long long int for_all_queries_min_dist_counter;  // node specific

// total min distances and total real distances
unsigned long long int total_computed_min_dists = 0; // for all nodes
unsigned long long int total_computed_real_dists = 0;
unsigned long long int **dists = NULL;

// for a node
extern int bsf_change_counter;               // total times of bsf changes
extern int bsf_share_counter;                // total times that i bcast-send a bsf to others
extern int bsf_receives_counter;             // total times that i bcast-receive a bsf from others
extern int bsf_receives_wrong_query_counter; // total times that i receive a bcast and the sequence number is different than my current processed query

// for all nodes
int total_bsf_change_counter = 0; // for all nodes
int total_bsf_share_counter = 0;
int total_bsf_receives_counter = 0;
int total_bsf_receives_wrong_query_counter = 0;
int **bsfs_stats = NULL;

int *bcasts_per_query = NULL;

double *queries_times = NULL; // times for each query independent [but this is kept 2ble times..]
double *min_queries_times = NULL;
double *max_queries_times = NULL;
float *query_final_results = NULL;

/*This is times statistics about query threads*/
struct timeval *threads_start_time = NULL;
struct timeval *threads_curr_time = NULL;
double threads_total_difference = 0.0;
double *threads_total_difference_stats;

query_statistics_t *queries_statistics_manol;

int DEBUG_PRINT_FLAG = 0;

// Chatzakis stuff
int query_mode = 0;
int file_part_to_load = 0;
int distributed_queries_initial_burst = 1;
isax_index *query_index = NULL;

// Chatzakis - Query Prediction Stats
char produce_query_prediction_statistics = 0; //! false/true!
long int per_query_total_bsf_changes = 0;
char *query_statistics_filename = "YANDEX_query_prediction_stats_current.txt";
FILE *query_statistics_file = NULL;
struct timeval bsf_start_time, bsf_end_time;
double elapsed_time_for_bsf = 0;
volatile long int per_query_leafs_visited = 0;
double per_node_score_greedy_alg = 0;
int per_node_queries_greedy_alg = 0;
double sum_of_per_query_times = 0;

// Chatzakis - Query Statistics
char keep_per_query_times = 0;
char *query_times_output_file = "query_execution_times_sift_100"; //"query_execution_time_estimations_benchmark129.txt";

// Chatzakis - Query Grouping
query_group *guery_groups = NULL;
long int total_query_groups = 0;

// Chatzakis - Benchmark mode
char creating_benchmark = 0;
char *benchmark_estimations_filename = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark501.txt";

// Chatzakis - Per_subtree stats -- Per Query
per_subtree_stats **subtree_stats = NULL; // array[2^w * queries]
char *subtree_stats_filename = "./subtree_stats_per_query.txt";
FILE *subtree_stats_file = NULL;
char gather_subtree_stats = 0;

// Chatzakis - WS pq stats
char *pq_stats_filename = "./YANDEX_pq_stats_new.txt";
FILE *pq_stats_file = NULL;
char gather_pq_stats = 0;
char *pq_size_stats_filename = "./pq_size_stats.txt";

// Chatzakis - WS pq estimations
char *basis_function_filename = NULL;
int th_div_factor = 1;

// Chatzakis - Workstealing
int workstealing_chatzakis = 0;
int workstealing_share_bsf = 1;
int batches_to_send = 1;
float limit_factor = 0.8;

// Chatakis - Partial Data Replication
long int node_groups_number;
pdr_node_group *node_groups = NULL;
long int node_group_total_nodes;
int bsf_sharing_pdr = 0;

int gather_init_bsf_only = 0;
char *init_bsf_filename = "_new_YANDEX100_initial_bsfs.txt";

//! WORKSTEALING PATCH
MPI_Request *global_helper_requests = NULL;

//! first time bsf sharing
int first_time_flag = 1;

int preprocess_index_data = 0;

/**
 * @brief Create a MPI type for system-wide BSF
 *
 */
void create_mpi_type()
{
    int lengths[2] = {1, 1};

    const MPI_Aint displacements[2] = {0, sizeof(int)};
    MPI_Datatype types[2] = {MPI_INT, MPI_FLOAT}; // the number of the query, the BSF value
    MPI_Type_create_struct(2, lengths, displacements, types, &bsf_msg_type);
    MPI_Type_commit(&bsf_msg_type);
}

/**
 * @brief The fun begins!
 *
 * @param argc
 * @param argv
 * @return int
 */
int main(int argc, char **argv)
{
    DRESS_chatzakis(argc, argv);
    return 0;
}

/*manolis:
receive final answer for a query
take the partial answer from each node for each query and keep the smallest distance
this distance is the final answer for the query
*/
void recv_final_result(int queries_size)
{
    float recv_results[queries_size]; //[MC] - here the results gotten from other processes are put

    float results[comm_sz][queries_size];
    MPI_Status status;

    // get the queries results from each node
    COUNT_MPI_TIME_START
    for (int i = 0; i < comm_sz; i++)
    {
        MPI_Recv(&(recv_results[0]), queries_size, MPI_FLOAT, MPI_ANY_SOURCE, QUERIES_RESULTS_TAG, MPI_COMM_WORLD, &status);

        for (int j = 0; j < queries_size; ++j)
        {
            results[status.MPI_SOURCE][j] = recv_results[j];
        }
    }
    COUNT_MPI_TIME_END

    for (int query = 0; query < queries_size; ++query)
    {
        int process = 0;
        float min_distance = results[process][query];

        for (process = 1; process < comm_sz; ++process)
        {
            if (results[process][query] < min_distance)
            {
                min_distance = results[process][query];
            }
        }

        printf("Query [%d] -> min distance: [%f]\n", query, min_distance);
    }
}

void collect_threads_total_difference_stats()
{
    MPI_Request request;
    MPI_Isend(&threads_total_difference, 1, MPI_DOUBLE, MASTER, THREADS_MAX_DIFFERENCE_TAG, MPI_COMM_WORLD, &request);

    if (my_rank == 0)
    {
        threads_total_difference_stats = malloc(sizeof(double) * comm_sz);
        for (int i = 0; i < comm_sz; i++)
        {
            MPI_Recv(&threads_total_difference_stats[i], 1, MPI_DOUBLE, i, THREADS_MAX_DIFFERENCE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }
    MPI_Wait(&request, MPI_STATUS_IGNORE);
}

void collect_queries_times(int queries_size)
{
    if (my_rank != 0)
    {
        MPI_Send(queries_times, queries_size, MPI_DOUBLE, MASTER, QUERIES_TIMES_STATS_TAG, MPI_COMM_WORLD);
        return;
    }

    double **queries_times_stats = malloc(sizeof(double *) * comm_sz);
    for (int i = 0; i < comm_sz; i++)
    {
        queries_times_stats[i] = malloc(sizeof(double) * queries_size);
    }

    for (int i = 0; i < queries_size; i++)
    {
        queries_times_stats[0][i] = queries_times[i];
    }

    double recv_times[queries_size];
    for (int i = 1; i < comm_sz; i++)
    {
        int source = i;
        MPI_Recv(&recv_times, queries_size, MPI_DOUBLE, source, QUERIES_TIMES_STATS_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int j = 0; j < queries_size; ++j)
        {
            queries_times_stats[source][j] = recv_times[j];
        }
    }

    min_queries_times = malloc(sizeof(double) * queries_size);
    max_queries_times = malloc(sizeof(double) * queries_size);

    for (int i = 0; i < queries_size; i++)
    {
        min_queries_times[i] = FLT_MAX;
        max_queries_times[i] = 0;
    }

    for (int query = 0; query < queries_size; query++)
    {
        double min_query_time = queries_times_stats[0][query];

        for (int node = 1; node < comm_sz; node++)
        {
            if (queries_times_stats[node][query] < min_query_time)
            {
                min_query_time = queries_times_stats[node][query];
            }
        }
        min_queries_times[query] = min_query_time;
    }

    for (int query = 0; query < queries_size; query++)
    {
        double max_query_time = queries_times_stats[0][query];

        for (int node = 1; node < comm_sz; node++)
        {

            if (queries_times_stats[node][query] > max_query_time)
            {
                max_query_time = queries_times_stats[node][query];
            }
        }
        max_queries_times[query] = max_query_time;
    }

    /*if (my_rank == MASTER)
    {
        for (int query = 0; query < queries_size; query++)
        {
            printf("%f\n", max_queries_times[query] / 1000000);
        }
    }*/
}

// collect lower bound and real distances for each node
void collect_dists_stats()
{
    MPI_Request request;

    unsigned long long int dists_to_send[DISTS_STATS_NUM] = {for_all_queries_min_dist_counter, for_all_queries_real_dist_counter};
    MPI_Isend(&dists_to_send[0], DISTS_STATS_NUM, MPI_UNSIGNED_LONG_LONG, MASTER, DISTS_STATS_TAG, MPI_COMM_WORLD, &request);

    if (my_rank == 0)
    {
        MPI_Status status;

        unsigned long long int recv_dists[DISTS_STATS_NUM];

        for (int i = 0; i < comm_sz; i++)
        {

            MPI_Recv(&(recv_dists[0]), DISTS_STATS_NUM, MPI_UNSIGNED_LONG_LONG, MPI_ANY_SOURCE, DISTS_STATS_TAG, MPI_COMM_WORLD, &status);

            for (int j = 0; j < DISTS_STATS_NUM; ++j)
            {
                dists[status.MPI_SOURCE][j] = recv_dists[j];
            }
        }

        // unsigned long long int dists[dists_num] = {for_all_queries_min_dist_counter, for_all_queries_real_dist_counter};
        unsigned long long int dists_result[DISTS_STATS_NUM] = {0, 0};
        for (int dists_i = 0; dists_i < DISTS_STATS_NUM; ++dists_i)
        {
            for (int proc_id = 0; proc_id < comm_sz; ++proc_id)
            {
                dists_result[dists_i] += dists[proc_id][dists_i];
            }
        }

        total_computed_min_dists = dists_result[0];
        total_computed_real_dists = dists_result[1];

        printf("Total min dists: %llu\n", dists_result[0]);
        printf("Total real dists: %llu\n", dists_result[1]);
    }
    MPI_Wait(&request, MPI_STATUS_IGNORE);
}

/*
Other statistics about how many time a node change its bsf
how many time sends the bsf to the other node, if we share the bsf
etc...
*/
void collect_bsf_stats()
{
    MPI_Request request;

    int dsf_to_send[BSF_STATS_NUM] = {bsf_change_counter, bsf_share_counter, bsf_receives_counter, bsf_receives_wrong_query_counter};
    MPI_Isend(dsf_to_send, BSF_STATS_NUM, MPI_INT, MASTER, BSF_STATS_TAG, MPI_COMM_WORLD, &request);

    if (my_rank == 0)
    {
        MPI_Status status;

        int recv_bsf_stats[BSF_STATS_NUM];

        for (int i = 0; i < comm_sz; i++)
        {

            MPI_Recv(recv_bsf_stats, BSF_STATS_NUM, MPI_INT, MPI_ANY_SOURCE, BSF_STATS_TAG, MPI_COMM_WORLD, &status);

            for (int j = 0; j < BSF_STATS_NUM; ++j)
            {
                bsfs_stats[status.MPI_SOURCE][j] = recv_bsf_stats[j];
            }
        }

        // unsigned long long int dists[dists_num] = {for_all_queries_min_dist_counter, for_all_queries_real_dist_counter};
        int bsf_result[BSF_STATS_NUM] = {0, 0, 0, 0};
        for (int bsf_i = 0; bsf_i < BSF_STATS_NUM; ++bsf_i)
        {
            for (int proc_id = 0; proc_id < comm_sz; ++proc_id)
            {
                bsf_result[bsf_i] += bsfs_stats[proc_id][bsf_i];
            }
        }

        total_bsf_change_counter = bsf_result[0];
        total_bsf_share_counter = bsf_result[1];
        total_bsf_receives_counter = bsf_result[2];
        total_bsf_receives_wrong_query_counter = bsf_result[3];

        printf("total_bsf_change_counter: %d\n", total_bsf_change_counter);
        printf("total_bsf_share_counter: %d\n", total_bsf_share_counter);
        printf("total_bsf_receives_counter: %d\n", total_bsf_receives_counter);
        printf("total_bsf_receives_wrong_query_counter: %d\n", total_bsf_receives_wrong_query_counter);
    }

    MPI_Wait(&request, MPI_STATUS_IGNORE);
}

/**
 * @brief Prints a basic info about the current process running
 *
 * @param my_rank
 */
void print_process_bind_info(int my_rank)
{
    char hostname[1024];
    gethostname(hostname, 1023);
    printf("[MC] - Process [%d] runs on machine [%s]\n", my_rank, hostname);
}

/**
 * @brief Calculates how many data series will each process get
 *
 * @param dataset_size Total dataset size
 * @return long int Data Series per process
 */
long int split_data_series_for_processes(long int dataset_size)
{
    // time series per process
    long int dataset_size_per_process = dataset_size / comm_sz;
    long double times_series_per_process = ((long double)dataset_size) / comm_sz;

    if (IS_EVEN(comm_sz))
    {
        if (my_rank == MASTER)
        {
            printf("[MC] - Process with rank [%d] dataset_size (# number of data series): %ld\n", my_rank, dataset_size);                                     // On 20GB file -> #data series 20.971.520
            printf("[MC] - Process with rank [%d] times_series_per_process (# number of data series per process): %Lf\n", my_rank, times_series_per_process); // 5.242.880
        }

        if (times_series_per_process != (long int)times_series_per_process)
        {

            if (my_rank == MASTER)
            {
                printf("[MC] - Process with rank [%d]: Something went wrong with the data series split. Exiting...\n", my_rank);
            }

            MPI_Finalize();
            exit(-1);
        }
    }
    else
    {
        printf("[MC] - Odd number of nodes: (%d)\n", comm_sz);

        if (IS_LAST_NODE(my_rank, comm_sz))
        { // the last node must take one time series extra
            dataset_size_per_process++;
        }

        printf("[MC] - Process with rank [%d]: I will process %ld num of times series!\n", my_rank, dataset_size_per_process);
    }

    return dataset_size_per_process;
}

/**
 * @brief Get the options for MESSI object
 *
 * @param argc Argument count
 * @param argv Arguments
 */
void get_options_for_MESSI(int argc, char **argv)
{
    while (1)
    {
        static struct option long_options[] = {
            {"use-index", no_argument, 0, 'a'},
            {"initial-lbl-size", required_argument, 0, 'b'},
            {"complete-type", required_argument, 0, 'c'},
            {"dataset", required_argument, 0, 'd'},
            {"total-loaded-leaves", required_argument, 0, 'e'},
            {"flush-limit", required_argument, 0, 'f'},
            {"aggressive-check", no_argument, 0, 'g'},
            {"help", no_argument, 0, 'h'},
            {"initial-fbl-size", required_argument, 0, 'i'},
            {"serial", no_argument, 0, 'j'},
            {"queries-size", required_argument, 0, 'k'},
            {"leaf-size", required_argument, 0, 'l'},
            {"min-leaf-size", required_argument, 0, 'm'},
            {"tight-bound", no_argument, 0, 'n'},
            //{"read-thread", required_argument, 0, 'o'}, //[MC] -> Read Thread can be set by -o, but for grouping we also set it with I
            {"index-workers", required_argument, 0, 'o'}, //[MC] addition
            {"index-path", required_argument, 0, 'p'},
            {"queries", required_argument, 0, 'q'},
            {"read-block", required_argument, 0, 'r'},
            {"minimum-distance", required_argument, 0, 's'},
            {"timeseries-size", required_argument, 0, 't'},
            {"min-checked-leaves", required_argument, 0, 'u'},
            {"in-memory", no_argument, 0, 'v'},
            {"cpu-type", required_argument, 0, 'w'}, //[MC] not to be used
            {"sax-cardinality", required_argument, 0, 'x'},
            {"function-type", required_argument, 0, 'y'},
            {"dataset-size", required_argument, 0, 'z'},
            {"k-size", required_argument, 0, '0'},
            {"knn-label-set", required_argument, 0, '1'},
            {"knn-label-size", required_argument, 0, '2'},
            {"knn", no_argument, 0, '3'},
            {"topk", no_argument, 0, '4'},
            {"ts-group-length", required_argument, 0, '5'},
            {"backoff-power", required_argument, 0, '6'},
            {"mpi-already-splitted-dataset", required_argument, 0, '7'}, // all the nodes has the same file and must seek or dataset is already splitted?
            {"file-label", required_argument, 0, '8'},                   // file-label
            {"output-file", required_argument, 0, '9'},                  // file-label
            {"share-bsf", required_argument, 0, '`'},
            {"verbose", required_argument, 0, '~'},
            {"all-nodes-index-all-dataset", required_argument, 0, '!'},
            {"simple-work-stealing", required_argument, 0, '@'},
            {"query-mode", required_argument, 0, 'A'},                        // parallel //[MC] addition
            {"parallel-query-workers", required_argument, 0, 'B'},            //[MC] addition
            {"part-to-load", required_argument, 0, 'C'},                      //[MC] addition
            {"distributed-queries-initial-burst", required_argument, 0, 'D'}, //[MC] addition
            {"benchmark_estimations_filename", required_argument, 0, 'E'},    //[MC] addition
            {"basis-function-filename", required_argument, 0, 'F'},
            {"th-div-factor", required_argument, 0, 'G'},
            {"workstealing", required_argument, 0, 'J'},
            {"batches-to-send", required_argument, 0, 'K'}, // l m n o p
            {"share-bsf-chatzakis", required_argument, 0, 'L'},
            {"node-groups", required_argument, 0, 'M'},
            {"preprocessing", required_argument, 0, 'N'},
            {"query-workers", required_argument, 0, 'Q'}, //[MC] addition
            {NULL, 0, NULL, 0}};

        /* getopt_long stores the option index here. */
        int option_index = 0;
        int c = getopt_long(argc, argv, "",
                            long_options, &option_index);
        if (c == -1)
            break;
        switch (c)
        {
        case 'j':
            serial_scan = 1;
            break;

        case 'g':
            aggressive_check = 1;
            break;

        case 's':
            minimum_distance = atof(optarg);
            break;

        case 'n':
            tight_bound = 1;
            break;

        case 'e':
            total_loaded_leaves = atoi(optarg);
            break;

        case 'c':
            complete_type = atoi(optarg);
            break;

        case 'q':
            queries = optarg;
            break;

        case 'k':
            queries_size = atoi(optarg);
            break;

        case 'd':
            dataset = optarg;
            break;

        case 'p':
            index_path = optarg;
            break;

        case 'z':
            dataset_size = atoi(optarg);
            break;

        case 't':
            time_series_size = atoi(optarg);
            break;

        case 'x':
            sax_cardinality = atoi(optarg);
            break;

        case 'l':
            leaf_size = atoi(optarg);
            break;

        case 'm':
            min_leaf_size = atoi(optarg);
            break;

        case 'b':
            initial_lbl_size = atoi(optarg);
            break;

        case 'f':
            flush_limit = atoi(optarg);
            break;

        case 'u':
            min_checked_leaves = atoi(optarg);
            break;

        case 'w':
            cpu_control_type = atoi(optarg);
            break;

        case 'y':
            function_type = atoi(optarg);
            break;

        case 'i':
            initial_fbl_size = atoi(optarg);
            break;

        case 'o':
            maxreadthread = atoi(optarg);
            break;

        case 'r':
            read_block_length = atoi(optarg);
            break;

        case '0':
            k_size = atoi(optarg);
            break;

        case '1':
            labelset = optarg;
            break;

        case '2':
            labelsize = atoi(optarg);

        case '3':
            knnlabel = 1;

        case '4':
            topk = 1;
            break;

        case '5':
            ts_group_length = atoi(optarg);
            break;

        case '6':
            if (atoi(optarg) == -1)
            {
                // backoff_time = 0;
                backoff_multiplier = 0;
                // printf ("backoff_time = [%d] --> [2^%d]\n", backoff_time, atoi(optarg));
            }
            else
            {
                // backoff_time = 1 << atoi(optarg);
                backoff_multiplier = atoi(optarg);
                // printf ("backoff_time = [%d] --> [2^%d]\n", backoff_time, atoi(optarg));
            }
            break;

        case '7':
            if (strcmp(optarg, "yes") == 0)
            {
                mpi_already_splitted_dataset = 1;
            }
            else if (strcmp(optarg, "no") == 0)
            {
                mpi_already_splitted_dataset = 0;
            }
            else
            {
                printf("Unrecognized value for --mpi_already_splitted_dataset. Values: [yes/no]\n");
                exit(1);
            }
            // printf("mpi_already_splitted_dataset: [%d]\n", mpi_already_splitted_dataset);
            break;

        case '8':
            file_label = optarg;
            break;

        case '9':
            // output_file = optarg; [mc change]
            output_file = strdup(optarg);
            break;

        case '`':
            if (strcmp(optarg, "bsf-dont-block-per-query") == 0)
            { // each node can run queries independent, so node 0 can be on query 1 and node 1 can by on query 3 (if node 1 has less job to do to the previous queries)
                share_with_nodes_bsf = BSF_DONT_BLOCK_PER_QUERY;
            }
            else if (strcmp(optarg, "bsf-block-per-query") == 0)
            { // all the nodes process the same query, after we have a clean up phase, and all the nodes together proceed to the next query, etc.
                share_with_nodes_bsf = BSF_BLOCK_PER_QUERY;
            }
            else if (strcmp(optarg, "bsf-dont-share") == 0)
            {
                share_with_nodes_bsf = BSF_DONT_SHARE;
            }
            else
            {
                printf("Unrecognized value for --share-bsf. Values: [yes/no]\n");
                exit(1);
            }

            break;

        case '~':
            if (strcmp(optarg, "yes") == 0)
            {
                DEBUG_PRINT_FLAG = 1;
            }
            else if (strcmp(optarg, "no") == 0)
            {
                DEBUG_PRINT_FLAG = 0;
            }
            else
            {
                printf("Unrecognized value for --verbose. Values: [yes/no]\n");
                exit(1);
            }
            break;

        case '!':
            if (strcmp(optarg, "yes") == 0)
            {
                all_nodes_index_all_given_dataset = 1;
            }
            else if (strcmp(optarg, "no") == 0)
            {
                all_nodes_index_all_given_dataset = 0;
            }
            else
            {
                printf("Unrecognized value for --verbose. Values: [yes/no]\n");
                exit(1);
            }
            break;

        case '@':
            if (strcmp(optarg, "yes") == 0)
            {
                simple_work_stealing = 1;
            }
            else if (strcmp(optarg, "no") == 0)
            {
                simple_work_stealing = 0;
            }
            else
            {
                printf("Unrecognized value for --simple-work-stealing. Values: [yes/no]\n");
                exit(1);
            }
            break;

        case 'h':
            printf("Usage:\n\
                \t--dataset XX \t\t\tThe path to the dataset file\n\
                \t--queries XX \t\t\tThe path to the queries file\n\
                \t--dataset-size XX \t\tThe number of time series to load\n\
                \t--queries-size XX \t\tThe number of queries to do\n\
                \t--minimum-distance XX\t\tThe minimum distance we search (MAX if not set)\n\
                \t--use-index  \t\t\tSpecifies that an input index will be used\n\
                \t--index-path XX \t\tThe path of the output folder\n\
                \t--timeseries-size XX\t\tThe size of each time series\n\
                \t--sax-cardinality XX\t\tThe maximum sax cardinality in number of bits (power of two).\n\
                \t--leaf-size XX\t\t\tThe maximum size of each leaf\n\
                \t--min-leaf-size XX\t\tThe minimum size of each leaf\n\
                \t--initial-lbl-size XX\t\tThe initial lbl buffer size for each buffer.\n\
                \t--flush-limit XX\t\tThe limit of time series in memory at the same time\n\
                \t--initial-fbl-size XX\t\tThe initial fbl buffer size for each buffer.\n\
                \t--complete-type XX\t\t0 for no complete, 1 for serial, 2 for leaf\n\
                \t--total-loaded-leaves XX\tNumber of leaves to load at each fetch\n\
                \t--min-checked-leaves XX\t\tNumber of leaves to check at minimum\n\
                \t--tight-bound XX\tSet for tight bounds.\n\
                \t--aggressive-check XX\t\tSet for aggressive check\n\
                \t--serial\t\t\tSet for serial scan\n\
                \t--in-memory\t\t\tSet for in-memory search\n\
                \t--function-type\t\t\tSet for query answering type on disk\n\
                                \t\t\tADS+: 0\n\
                \t\t\tParIS+: 1\n\
                \t\t\tnb-ParIS+: 2\n\n\
                \t\t\tin memory  traditional exact search: 0\n\
                \t\t\tADS+: 1\n\
                \t\t\tParIS-TS: 2\n\
                \t\t\tParIS: 4\n\
                \t\t\tParIS+: 6\n\
                \t\t\t\\MESSI-Hq: 7\n\
                \t\t\t\\MESSI-Sq: 8\n\
                \t--cpu-type\t\t\tSet for how many cores you want to used and in 1 or 2 cpu\n\
                \t--help\n\n\
                \tCPU type code:\t\t\t21 : 2 core in 1 CPU\n\
                \t\t\t\t\t22 : 2 core in 2 CPUs\n\
                \t\t\t\t\t41 : 4 core in 1 CPU\n\
                \t\t\t\t\t42 : 4 core in 2 CPUs\n\
                \t\t\t\t\t61 : 6 core in 1 CPU\n\
                \t\t\t\t\t62 : 6 core in 2 CPUs\n\
                \t\t\t\t\t81 : 8 core in 1 CPU\n\
                \t\t\t\t\t82 : 8 core in 2 CPUs\n\
                \t\t\t\t\t101: 10 core in 1 CPU\n\
                \t\t\t\t\t102: 10 core in 2 CPUs\n\
                \t\t\t\t\t121: 12 core in 1 CPU\n\
                \t\t\t\t\t122: 12 core in 2 CPUs\n\
                \t\t\t\t\t181: 18 core in 1 CPU\n\
                \t\t\t\t\t182: 18 core in 2 CPUs\n\
                \t\t\t\t\t242: 24 core in 2 CPUs\n\
                \t\t\t\t\tOther: 1 core in 1 CPU\n\
                \t--topk\t\t\tSet for topk search\n\
                \t--knn\t\t\tSet for knn search\n");
            exit(1);

        case 'a':
            use_index = 1;
            break;

        case 'v':
            inmemory_flag = 1;
            break;

        /*[MC] - Addition start*/
        case 'I':
            maxreadthread = atoi(optarg);
            break;
        case 'Q':
            maxquerythread = atoi(optarg);
            break;
        case 'A':
            query_mode = atoi(optarg); // 0 is sequential, 1 is parallel, 2 is distributed
            break;
        case 'B':
            maxparallelquerythread = atoi(optarg);
            break;
        case 'C':
            file_part_to_load = atoi(optarg);
            break;
        case 'D':
            distributed_queries_initial_burst = atoi(optarg);
            break;
        case 'E':
            benchmark_estimations_filename = strdup(optarg);
            break;
        case 'F':
            basis_function_filename = strdup(optarg); /// gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt
            break;
        case 'G':
            th_div_factor = atoi(optarg);
            break;
        case 'J':
            workstealing_chatzakis = atoi(optarg);
            break;
        case 'K':
            batches_to_send = atoi(optarg);
            break;
        case 'L':
            bsf_sharing_pdr = atoi(optarg);
            break;
        case 'M':
            node_groups_number = atoi(optarg);
            break;
        case 'N':
            preprocess_index_data = atoi(optarg);
            break;
        /*[MC] - Addition end*/
        default:
            exit(-1);
            break;
        }
    }

    if (mpi_already_splitted_dataset == -1)
    {
        printf("--mpi_already_splitted_dataset [yes/no] is required option to run the distributed MESSI!!!\n");
        exit(1);
    }

    if (output_file == NULL)
    {
        printf("--output-file [filename] is required option to run the distributed MESSI!\n");
        exit(1);
    }

    if (share_with_nodes_bsf == -1)
    {
        printf("--share-bsf [yes/no] is required option to run the distributed MESSI!\n");
        exit(1);
    }
}

/* ------------------- Chatzakis Thesis Stuff ------------------- */

/**
 * @brief Helper function to save the results of each run to corresponding files
 *
 * @param times_num
 * @param times_nodes
 * @param times An array holding all the result times for every node
 */
void keep_info_to_files_chatzakis(int times_num, int times_nodes, double times[times_nodes][times_num])
{
    int x = 1000000;
    printf("opening file : %s\n", output_file);

    FILE *fp = fopen(output_file, "w+");
    if (!fp)
    {
        perror("Could not open the file to write the results");
        exit(-1);
    }

    for (int i = 0; i < comm_sz; i++)
    {

        for (int k = 0; k < times_num; k++)
        {
            fprintf(fp, "%f ", times[i][k] / x);
        }

        for (int k = 0; k < DISTS_STATS_NUM; k++)
        {
            fprintf(fp, "%llu ", dists[i][k]);
        }

        for (int k = 0; k < BSF_STATS_NUM; k++)
        {
            fprintf(fp, "%d", bsfs_stats[i][k]);

            if (k != BSF_STATS_NUM - 1)
            {
                fprintf(fp, " ");
            }
        }

        fprintf(fp, "\n");
    }

    fclose(fp);
}

/**
 * @brief Runs the classic instance of MESSI (this is called when commsz is 1)
 *
 * @param index_settings The index settings of the tree
 * @param num_of_time_series_per_process Number of ts to process
 */
void classic_MESSI_chatzakis(isax_index_settings *index_settings, long num_of_time_series_per_process)
{
    print_process_bind_info(my_rank);

    idx_1 = isax_index_init_inmemory_ekosmas(index_settings);

    // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
    tmp_raw_file = index_creation_pRecBuf_new_ekosmas(dataset, num_of_time_series_per_process, idx_1, SIMPLE_WORK_STEALING_CREATE_MY_INDEX);

    if (creating_benchmark)
    {
        create_benchmarks_chatzakis(idx_1);
        return;
    }

    if (gather_pq_stats)
    {
        pq_stats_file = fopen(pq_stats_filename, "w+");
        fprintf(pq_stats_file, "#query #bsf #th #total_pqs #per_pq_size\n");
    }

    // PHASE 3: Query Answering
    if (query_mode == SEQUENTIAL_QUERY_ANSWERING)
    {
        isax_query_binary_file_simple_chatzakis(queries, queries_size, idx_1, minimum_distance, NULL, &exact_search_ParISnew_inmemory_hybrid_ekosmas);
    }
    else if (query_mode == WORKSTEALING_SUPPORTED_QUERY_ANSWERING)
    {
        isax_query_binary_file_simple_chatzakis(queries, queries_size, idx_1, minimum_distance, basis_function_filename, &exact_search_workstealing_chatzakis);
    }
    else
    {
        printf("[MC] - Unsupported QA method for classic MESSI.\n");
        exit(EXIT_FAILURE);
    }

    COUNT_TOTAL_TIME_END

    int x = 1000000;
    printf("[MC] - [Rank : %d] - RESULTS\nTotal input time: [%f]\nTotal output time: [%f]\nFill rec bufs time: [%f]\nCreate tree index time: [%f]\nQuery_answering_time[%f]\nMpi_time: [%f]\nTotal_time: [%f]\nQueue Fill Time: [%f]\nQueue Processing Time: [%f]\nQueue Preprocessing Time: [%f]\n------------\n",
           my_rank,
           total_input_time / x,
           total_output_time / x,
           fill_rec_bufs_time / x,
           create_tree_index_time / x,
           query_answering_time / x,
           mpi_time / x,
           total_time / x,
           queue_fill_time / x,
           queue_process_time / x,
           pq_preprocessing_time / x);

    printf("[MC]: nodes-%d, dataset-%s, queries-%d\n", comm_sz, file_label, queries_size);
    printf("[MC]: Total-min-dists: %llu Total-real-dists: %llu change-bsf: %d share-bsf: %d receive-bsf: %d wrong-receives-bsf:%d\n", for_all_queries_min_dist_counter, for_all_queries_real_dist_counter, 0, 0, 0, 0);
    printf("-----\n");

    printf("[MC]: Opening file : %s\n", output_file);

    FILE *fp = fopen(output_file, "w+");
    if (!fp)
    {
        perror("Could not open the file to write the results");
        exit(-1);
    }

    fprintf(fp, "%f %f %f %f %f %f %f %f %f %f 0 %llu %llu 0 0 0 0\n", total_input_time / x, total_output_time / x, fill_rec_bufs_time / x, create_tree_index_time / x, query_answering_time / x, mpi_time / x, total_time / x, queue_fill_time / x, queue_process_time / x, pq_preprocessing_time / x, for_all_queries_min_dist_counter, for_all_queries_real_dist_counter);
    fclose(fp);

    if (keep_per_query_times)
    {
        fp = fopen(query_times_output_file, "w+");
        if (!fp)
        {
            perror("Could not open the file to write the results");
            exit(-1);
        }

        for (int i = 0; i < queries_size; i++)
        {
            fprintf(fp, "%f\n", queries_times[i] / x);
        }

        fclose(fp);
    }

    if (gather_pq_stats)
    {
        fclose(pq_stats_file);
    }

    MPI_Finalize();
}

/**
 * @brief Initialize the statistics for all queries
 *
 */
void initialize_queries_statistics_chatzakis()
{
    queries_statistics_manol = malloc(sizeof(query_statistics_t) * queries_size);

    for (int i = 0; i < queries_size; i++)
    {
        queries_statistics_manol[i].final_bsf = -1;
        queries_statistics_manol[i].initial_bsf = -1;
        queries_statistics_manol[i].total_time = -1;
        queries_statistics_manol[i].real_distances = -1;
        queries_statistics_manol[i].lower_bound_distances = -1;
    }
}

/**
 * @brief Processes the results
 *
 * @param request MPIreq for basic MPI communication
 */
void process_results_chatzakis(MPI_Request request)
{
    dists = malloc(sizeof(unsigned long long int *) * comm_sz);
    for (int i = 0; i < comm_sz; ++i)
    {
        dists[i] = malloc(sizeof(unsigned long long int) * DISTS_STATS_NUM);
    }

    bsfs_stats = malloc(sizeof(int *) * comm_sz);
    for (int i = 0; i < comm_sz; ++i)
    {
        bsfs_stats[i] = malloc(sizeof(int) * BSF_STATS_NUM);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    fflush(stdout);

    collect_dists_stats();
    collect_bsf_stats();

    int x = 1000000;
    int interested_times_num = 17;
    int query_time_index = 4;
    int buffer_time_index = 2;
    int index_time_index = 3;
    int communication_time_index = 8; // or waiting time
    int query_preprocessing_time_index = 9;
    int per_node_queries_greedy_index = 10;
    int per_node_score_greedy_index = 11;
    int sum_of_per_query_times_index = 12;
    int workstealing_index = 13;
    int pq_fill_time_index = 14;
    int pq_preprocessing_time_index = 15;
    int pq_processing_time_index = 16;

    // This array now has come additional things apart from times, related to the greedy technique
    double times_to_send[] = {total_input_time, total_output_time, fill_rec_bufs_time, create_tree_index_time, query_answering_time, mpi_time, total_time, queries_result_collection_time, communication_time, query_preprocessing_time, per_node_queries_greedy_alg * x, per_node_score_greedy_alg * x, sum_of_per_query_times_index, workstealing_qa_time, queue_fill_time, pq_preprocessing_time, queue_process_time};
    MPI_Isend(times_to_send, interested_times_num, MPI_DOUBLE, MASTER, TIMES_TAG, MPI_COMM_WORLD, &request);

    // COLLECT TIMES
    double times[comm_sz][interested_times_num];

    // process_rank, total_input_time, total_output_time, total_time and mpi_time
    if (my_rank == MASTER)
    {
        double times_recv[interested_times_num];
        MPI_Status status;

        for (int i = 0; i < comm_sz; ++i)
        {
            MPI_Recv(&(times_recv[0]), interested_times_num, MPI_DOUBLE, MPI_ANY_SOURCE, TIMES_TAG, MPI_COMM_WORLD, &status);
            // printf("status.MPI_SOURCE : %d\n", status.MPI_SOURCE);

            for (int j = 0; j < interested_times_num; ++j)
            {
                times[status.MPI_SOURCE][j] = times_recv[j];
            }
        }
    }

    collect_queries_times(queries_size);      //[MC] -> to see
    collect_threads_total_difference_stats(); //[MC] -> to see

    MPI_Wait(&request, MPI_STATUS_IGNORE); //[MC] -> to see

    MPI_Finalize(); // manol [MC] removed from bottom

    // [MC]: Logs the final results - up to this point I think only master is gonna log
    if (my_rank == MASTER)
    {
        printf("================ MASTER NODE ENDING OUTPUT ================\n");

        // int x = 1000000;
        printf("[nodes-%d], [dataset-%s], [queries-%d]\n", comm_sz, file_label, queries_size);

        for (int i = 0; i < comm_sz; i++)
        {
            printf("[%i] Buf: %f Ind: %f QA: %f ", i, times[i][buffer_time_index] / x, times[i][index_time_index] / x, times[i][query_time_index] / x);
            printf("Comm: %f QueryPrePro: %f ", times[i][communication_time_index] / x, times[i][query_preprocessing_time_index] / x);
            printf("WorkStealing: %f ", times[i][workstealing_index] / x);
            printf("PQ Fill: %f PQPrePro: %f PQProc: %f ", times[i][pq_fill_time_index] / x, times[i][pq_preprocessing_time_index] / x, times[i][pq_processing_time_index] / x);
            printf("\n");
        }

        keep_info_to_files_chatzakis(interested_times_num, comm_sz, times);

        printf("===========================================================\n");
    }

    if (produce_query_prediction_statistics)
    {
        fclose(query_statistics_file);
    }

    return;
}

/**
 * @brief Create a query index object
 *
 * @return isax_index* query_index
 */
isax_index *create_query_index_chatzakis()
{
    isax_index_settings *index_settings = isax_index_settings_init(index_path, time_series_size, 16, sax_cardinality, 100 /*LEAF SIZE*/, 100 /*MIN LEAF SIZE*/, 100 /*INITIAL LEAF BUFFER SIZE*/, flush_limit /*FLUSH LIMIT*/, initial_fbl_size /*INITIAL FBL BUFFER SIZE*/, total_loaded_leaves /* Leaves to load at each fetch*/, tight_bound /*Tightness of leaf bounds*/, aggressive_check /*aggressive check*/, 1 /*???? EKOSMAS ????*/, inmemory_flag);
    isax_index *query_index = isax_index_init_inmemory_ekosmas(index_settings);

    float *raw_queries_file = index_creation_pRecBuf_new_ekosmas(queries, queries_size, query_index, SIMPLE_WORK_STEALING_CREATE_MY_INDEX);

    get_tree_statistics_chatz(query_index);
    traverse_query_tree_buffers_chatzakis(query_index);

    return query_index;
}

void traverse_query_tree_buffers_chatzakis(isax_index *index)
{
    long int empty_subtrees_buffers = 0;
    long int curr_leaf_num = 0;
    long int total_leafs_nodes = 0;

    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            empty_subtrees_buffers++;
            continue;
        }

        total_leafs_nodes += find_total_leafs_nodes((isax_node *)current_fbl_node->node);
    }

    total_query_groups = total_leafs_nodes;
    guery_groups = malloc(sizeof(query_group) * total_query_groups);

    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {
        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        isax_node *subtree_root = (isax_node *)current_fbl_node->node;
        traverse_root_subtree_chatzakis(subtree_root, &curr_leaf_num);
    }

    print_query_groups();

    check_queries_distances(index);
}

void traverse_root_subtree_chatzakis(isax_node *curr_root, long int *leaf_num)
{
    if (curr_root == NULL)
    {
        return;
    }

    if (curr_root->is_leaf && curr_root->buffer != NULL)
    {
        ts_type *base_query;
        ts_type *curr_query;

        printf("[MC] - Reached the %ld leaf.. Creating query group of %d queries.\n", *leaf_num, curr_root->buffer->partial_buffer_size);

        query_group *qp = &guery_groups[*leaf_num];
        qp->group_size = curr_root->buffer->partial_buffer_size;
        qp->queries = malloc(sizeof(query_data) * curr_root->buffer->partial_buffer_size);

        for (int i = 0; i < curr_root->buffer->partial_buffer_size; i++)
        {
            if (i == 0)
            {
                base_query = &(tmp_raw_file[*curr_root->buffer->partial_position_buffer[i]]);

                qp->query_representative = base_query;

                qp->queries[i].query = base_query;
                qp->queries[i].dist_from_base = 0;
            }
            else
            {
                curr_query = &(tmp_raw_file[*curr_root->buffer->partial_position_buffer[i]]);

                float dist = ts_euclidean_distance_SIMD(base_query, curr_query, time_series_size, /*bsf*/ FLT_MAX);

                qp->queries[i].query = curr_query;
                qp->queries[i].dist_from_base = dist;
            }

            qp->queries[i].id = 0;

            // stats init
            qp->queries[i].stats.final_bsf = FLT_MAX;
            qp->queries[i].stats.initial_bsf = FLT_MAX;
            qp->queries[i].stats.lower_bound_distances = 0;
            qp->queries[i].stats.real_distances = 0;
            qp->queries[i].stats.total_time = 0;
        }

        (*leaf_num)++;
    }

    traverse_root_subtree_chatzakis(curr_root->left_child, leaf_num);
    traverse_root_subtree_chatzakis(curr_root->right_child, leaf_num);

    return;
}

void print_query_groups()
{
    long int total_queries = 0;
    for (int i = 0; i < total_query_groups; i++)
    {
        printf("[MC] - Visiting query group %d\n", i);

        query_group g = guery_groups[i];

        printf("== Group Size: %ld\n", g.group_size);
        for (int k = 0; k < g.group_size; k++)
        {
            printf("==== Dist(q[%d], base) = %f\n", k, g.queries[k].dist_from_base);
        }

        total_queries += g.group_size;
    }

    printf("[MC] -- Total Queries of all groups: %ld\n", total_queries);
}

void check_queries_distances(isax_index *index)
{
    int q_num = queries_size;
    if (comm_sz == 1)
        fprintf(stderr, ">>> Performing queries in file: %s\n", queries);

    FILE *ifile;
    ifile = fopen(queries, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", queries);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);
    file_position_type total_records = sz / index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num)
    {
        fprintf(stderr, "File %s has only %llu records!\n", queries, total_records);
        exit(-1);
    }

    int q_loaded = 0;

    ts_type **queries = load_queries_from_file_chatzakis(index, q_num, ifile);

    for (int i = 0; i < 100; i++)
    {
        for (int j = 0; j < 100; j++)
        {
            printf("[MC] - D(%d,%d) = %f\n", i, j, ts_euclidean_distance_SIMD(queries[i], queries[j], time_series_size, /*bsf*/ FLT_MAX));
        }
    }
}

void initial_log_chatzakis()
{
    printf("================ MASTER NODE STARTING OUTPUT ================\n");
    printf("[Total Processes, File Label]: [%d, %s]\n", comm_sz, file_label);
    printf("[Leaf Size, Min Leaf Size, Read Block, Flush Limit]: [%d, %d, %d, %d]\n", leaf_size, min_leaf_size, read_block_length, flush_limit);
    printf("[Dataset, Queries]: [%s, %s]\n", dataset, queries);
    printf("Time series size: %d\n", time_series_size);
    printf("System-Wide BSF: %d\n", share_with_nodes_bsf);
    printf("[Dataset_Size, Queries_Size]: [%lu, %lu]\n", dataset_size, queries_size);
    printf("[Index Workers, Query Workers]: [%d, %d]\n", maxreadthread, maxquerythread);
    printf("Query Mode: %d\n", query_mode);
    // if (query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC ||
    //     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_SORTED_QUERIES ||
    //     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR)
    //{
    printf("Distributed Queries Burst: %d\n", distributed_queries_initial_burst);
    //}
    printf("Chunk to load: %d\n", file_part_to_load);
    printf("Gather query prediction statistics: %d\n", produce_query_prediction_statistics);
    printf("All Nodes Index Dataset: %d\n", all_nodes_index_all_given_dataset);
    // if (query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY || query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_SORTED)
    //{
    printf("Query Estimations Filename: %s\n", benchmark_estimations_filename);
    //}
    // if (query_mode == WORKSTEALING_SUPPORTED_QUERY_ANSWERING)
    //{
    printf("TH Division Factor: %d\n", th_div_factor);
    printf("Basis Func Filename: %s\n", basis_function_filename);
    printf("Workstealing: %d\n", workstealing_chatzakis);
    //}

    printf("Total node groups: %d\n", node_groups_number);

    printf("=============================================================\n");
}

void benchmark1(isax_index *index, dress_query *seismic_queries, int size)
{
    int total_queries_to_include = 800 + 1;
    int selected_q_ids[total_queries_to_include];

    for (int i = 0; i < total_queries_to_include - 1; i++)
    {
        selected_q_ids[i] = i;
    }
    // selected_q_ids[0] = 10;
    selected_q_ids[total_queries_to_include - 1] = size - 1; // the most difficult one

    printf("Benchmarck will save to the file the following IDs...\n");
    for (int i = 0; i < total_queries_to_include; i++)
    {
        int q_id = selected_q_ids[i];
        printf("%d ", q_id);
    }
    printf("\n");

    // create the files:
    char *benchmark_filename = "/gpfs/scratch/chatzakis/benchmark801_astro_len256_znorm.bin"; //!
    char *benchmark_estimations_filename = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/Odyssey/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_astro_benchmark801.txt";
    FILE *bf = fopen(benchmark_filename, "wb");
    if (!bf)
    {
        perror("Could not create bin file...\n");
        exit(EXIT_FAILURE);
    }

    FILE *est_f = fopen(benchmark_estimations_filename, "w+");
    if (!est_f)
    {
        perror("Could not create estimations file...\n");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < total_queries_to_include; i++)
    {
        int q_id = selected_q_ids[i];
        printf("Writing query %d to bin benchmark and estimation to file.\n", q_id);

        fwrite(seismic_queries[q_id].query, sizeof(ts_type), index->settings->timeseries_size, bf);
        fprintf(est_f, "%f\n", seismic_queries[q_id].initial_estimation);
    }

    fclose(bf);
    fclose(est_f);
}

void benchmark2(isax_index *index, dress_query *seismic_queries, int size)
{
    int easy_part = 250, diff_part = 51;
    int total_queries_to_include = easy_part + diff_part;
    int selected_q_ids[total_queries_to_include];

    for (int i = 0; i < easy_part; i++)
    {
        selected_q_ids[i] = i;
    }

    int curr_index = easy_part;
    for (int i = 0; i < diff_part; i++)
    {
        selected_q_ids[curr_index++] = 4999 + i;
    }

    printf("Benchmarck will save to the file the following IDs...\n");
    for (int i = 0; i < total_queries_to_include; i++)
    {
        int q_id = selected_q_ids[i];
        printf("%d ", q_id);
    }
    printf("\n");

    // create the files:
    char *benchmark_filename = "/gpfs/scratch/chatzakis/benchmark301_51d_seismic_len256_znorm.bin";
    char *benchmark_estimations_filename = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark301_51d.txt";
    FILE *bf = fopen(benchmark_filename, "wb");
    if (!bf)
    {
        perror("Could not create bin file...\n");
        exit(EXIT_FAILURE);
    }

    FILE *est_f = fopen(benchmark_estimations_filename, "w+");
    if (!est_f)
    {
        perror("Could not create estimations file...\n");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < total_queries_to_include; i++)
    {
        int q_id = selected_q_ids[i];
        printf("Writing query %d to bin benchmark and estimation to file.\n", q_id);

        fwrite(seismic_queries[q_id].query, sizeof(ts_type), index->settings->timeseries_size, bf);
        fprintf(est_f, "%f\n", seismic_queries[q_id].initial_estimation);
    }

    fclose(bf);
    fclose(est_f);
}

void create_benchmarks_chatzakis(isax_index *index)
{
    char benchmark = 0;

    char *seismic_queries1_file = "/gpfs/scratch/chatzakis/queries_ctrl100_astronomy_len256_znorm.bin";
    // char *seismic_queries1_execution_times = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/official_execution_times/query_execution_time_estimations_seismic1.txt";
    char *seismic_queries1_execution_times = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/Odyssey/ads/run_automation/query_analysis/predictions/predicted_execution_times/astro_100.txt";
    int size1 = 100;

    char *seismic_queries2_file = "/gpfs/scratch/chatzakis/generated/queries_ctrl5000_astro_len256_znorm.bin";
    // char *seismic_queries2_execution_times = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/official_execution_times/query_execution_time_estimations_seismic2.txt";
    char *seismic_queries2_execution_times = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/Odyssey/ads/run_automation/query_analysis/predictions/predicted_execution_times/astro_1K.txt";
    int size2 = 1000;

    printf("[MC] - System Creating Benchmarks...\n");

    FILE *qf = fopen(seismic_queries1_file, "rb");
    FILE *ef = fopen(seismic_queries1_execution_times, "r");
    if (!ef)
    {
        perror("Could not find the execution times file...\n");
        exit(EXIT_FAILURE);
    }
    dress_query *seismic_queries1 = load_dress_queries_from_file_chatzakis(index, size1, qf, ef);
    fclose(qf);
    fclose(ef);

    qf = fopen(seismic_queries2_file, "rb");
    ef = fopen(seismic_queries2_execution_times, "r");
    dress_query *seismic_queries2 = load_dress_queries_from_file_chatzakis(index, size2, qf, ef);
    fclose(qf);
    fclose(ef);

    // concat
    int size = size1 + size2;
    dress_query *seismic_queries = malloc(sizeof(dress_query) * size);

    memcpy(seismic_queries, seismic_queries1, size1 * sizeof(dress_query));
    memcpy(seismic_queries + size1, seismic_queries2, size2 * sizeof(dress_query));

    sort_dress_query_array_chatzakis(seismic_queries, size, compare_dress_queries_initEstTime_revs);
    // print_dress_queries_chatzakis(seismic_queries, size);

    switch (benchmark)
    {
    case 0:
        benchmark1(index, seismic_queries, size);
        break;
    case 1:
        benchmark2(index, seismic_queries, size);
        break;
    }

    return;
}

/* --------------------- PARTIAL DATA REPLICATION --------------------- */

long int split_data_series_for_node_groups(long int dataset_size, long int groups_to_create)
{
    long int dataset_size_per_process = dataset_size / groups_to_create;
    long double times_series_per_process = ((long double)dataset_size) / groups_to_create;

    if (IS_EVEN(groups_to_create) || groups_to_create == 1) //!
    {
        if (my_rank == MASTER)
        {
            printf("[MC] - Process with rank [%d] dataset_size (# number of data series): %ld\n", my_rank, dataset_size);                                     // On 20GB file -> #data series 20.971.520
            printf("[MC] - Process with rank [%d] times_series_per_process (# number of data series per process): %Lf\n", my_rank, times_series_per_process); // 5.242.880
        }

        if (times_series_per_process != (long int)times_series_per_process)
        {

            if (my_rank == MASTER)
            {
                printf("[MC] - Process with rank [%d]: Something went wrong with the data series split. Exiting...\n", my_rank);
            }

            MPI_Finalize();
            exit(-1);
        }
    }
    else
    {
        printf("[MC] - Odd number of groups: (%d)\n", groups_to_create);

        if (FIND_NODE_GROUP(my_rank, node_group_total_nodes) == groups_to_create - 1) //! check what is gonna happen if you give groups = 1
        {
            dataset_size_per_process++;
        }

        printf("[MC] - Process with rank [%d]: I will process %ld num of times series!\n", my_rank, dataset_size_per_process);
    }

    return dataset_size_per_process;
}

void log_node_group_info()
{
    for (int i = 0; i < node_groups_number; i++)
    {
        printf("Group [%d]: \n", node_groups[i].node_group_id);
        printf("    Time Series: %lu\n", node_groups[i].total_time_series);
        printf("    Total Nodes: %d\n", node_groups[i].total_nodes);
        printf("    Coordinator Node: %d\n", node_groups[i].coordinator_node);
        printf("    Nodes: [ ");
        for (int n = 0; n < node_groups[i].total_nodes; n++)
        {
            printf("%d ", node_groups[i].nodes[n]);
        }
        printf("]\n");
    }
}

void allocate_node_groups(long int time_series_per_node_group)
{
    // create the groups
    node_groups = malloc(sizeof(pdr_node_group) * node_groups_number);
    for (int i = 0; i < node_groups_number; i++)
    {
        node_groups[i].total_time_series = time_series_per_node_group;
        node_groups[i].node_group_id = i;
        node_groups[i].total_nodes = node_group_total_nodes;

        node_groups[i].coordinator_node = (i * node_group_total_nodes);
        node_groups[i].nodes = malloc(sizeof(int) * node_group_total_nodes);

        for (int n = 0; n < node_group_total_nodes; n++)
        {
            node_groups[i].nodes[n] = (i * node_group_total_nodes) + n;
        }
    }
}

/* --------------------- May 2022 - Chatzakis DRESS Layout --------------------- */
void DRESS_chatzakis(int argc, char **argv)
{
    read_block_length = 20000;
    ts_group_length = 1;
    backoff_multiplier = 1;

    if (produce_query_prediction_statistics)
    {
        query_statistics_file = fopen(query_statistics_filename, "w+");
        per_query_total_bsf_changes = 0;
        fprintf(query_statistics_file, "Query# BSF# BSFresult Leafs# TimeMS\n");
    }

    get_options_for_MESSI(argc, argv);

    INIT_STATS();

    isax_index_settings *index_settings = isax_index_settings_init(index_path /*INDEX DIRECTORY*/, time_series_size /* TIME SERIES SIZE*/, paa_segments /*PAA SEGMENTS*/, sax_cardinality /* SAX CARDINALITY IN BITS*/, leaf_size /*LEAF SIZE*/, min_leaf_size /*MIN LEAF SIZE*/, initial_lbl_size /*INITIAL LEAF BUFFER SIZE*/, flush_limit /*FLUSH LIMIT*/, initial_fbl_size /*INITIAL FBL BUFFER SIZE*/, total_loaded_leaves /* Leaves to load at each fetch*/, tight_bound /*Tightness of leaf bounds*/, aggressive_check /*aggressive check*/, 1 /*???? EKOSMAS ????*/, inmemory_flag); // new index
    CHECK_FOR_CORRECT_VERSION(function_type);

    queries_times = malloc(sizeof(double) * queries_size);

    threads_start_time = malloc(sizeof(struct timeval) * maxquerythread);
    threads_curr_time = malloc(sizeof(struct timeval) * maxquerythread);

    initialize_queries_statistics_chatzakis();

    COUNT_TOTAL_TIME_START
    COUNT_MPI_TIME_START
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided); // equivalent to MPI_Init(), configure the threads of threads that make MPI calls
    if (provided < MPI_THREAD_MULTIPLE)
    {
        printf("The threading support level is lesser than that demanded.\n");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    if (share_with_nodes_bsf || bsf_sharing_pdr)
    {
        create_mpi_type();

        communicators = malloc(sizeof(MPI_Comm) * comm_sz);
        requests = malloc(sizeof(MPI_Request) * comm_sz);
        shared_bsfs = malloc(sizeof(struct bsf_msg) * comm_sz);
        bcasts_per_query = malloc(sizeof(int) * comm_sz);

        // create the communicators, will be used later on BSF sharing among the nodes
        // each node will bcast(as sender) to one communicator and
        // and will bcast as (a receiver) to all the other communicators
        for (int i = 0; i < comm_sz; ++i)
        {
            MPI_Comm_dup(MPI_COMM_WORLD, &(communicators[i]));
            requests[i] = MPI_REQUEST_NULL;

            shared_bsfs[i].q_num = -1;
            shared_bsfs[i].bsf = -1.0;

            bcasts_per_query[i] = 0;
        }
    }

    COUNT_MPI_TIME_END

    if (my_rank == MASTER)
    {
        initial_log_chatzakis();
    }

    long int num_of_time_series_per_process = dataset_size;

    if (comm_sz == 1)
    {
        classic_MESSI_chatzakis(index_settings, num_of_time_series_per_process);
        return;
    }

    if (CLASSIC_DRESS_QA)
    {
        num_of_time_series_per_process = split_data_series_for_processes(dataset_size); // [MC] - assigns a chunk of data to the available nodes

        if (my_rank == MASTER)
        {
            printf("[MC] - From MASTER Node: Calculated time_series_per_process:  %lu\n", num_of_time_series_per_process);
        }

        print_process_bind_info(my_rank);

        idx_1 = isax_index_init_inmemory_ekosmas(index_settings);

        // PHASE 1: Fill in of Receive Bufers and PHASE 2: Index Creation
        raw_file_1 = index_creation_pRecBuf_new_ekosmas(dataset, num_of_time_series_per_process, idx_1, SIMPLE_WORK_STEALING_CREATE_MY_INDEX);
    }
    else
    {
        long int time_series_per_node_group;
        node_group_total_nodes = comm_sz / node_groups_number;

        time_series_per_node_group = split_data_series_for_node_groups(dataset_size, node_groups_number);

        if (my_rank == MASTER)
        {
            printf("[MC] - From MASTER Node: Calculated time_series_per_node_group:  %lu\n", time_series_per_node_group);
        }

        allocate_node_groups(time_series_per_node_group);

        if (my_rank == MASTER)
        {
            log_node_group_info();
        }

        idx_1 = isax_index_init_inmemory_ekosmas(index_settings);

        if (preprocess_index_data)
        {
            raw_file_1 = index_creation_node_groups_chatzakis_botao_prepro(dataset, time_series_per_node_group, idx_1, NO_PARALLELISM_IN_SUBTREE, node_groups, node_groups_number, node_group_total_nodes);
        }
        else
        {
            raw_file_1 = index_creation_node_groups_chatzakis(dataset, time_series_per_node_group, idx_1, NO_PARALLELISM_IN_SUBTREE, node_groups, node_groups_number, node_group_total_nodes);
        }
    }
    tmp_raw_file = raw_file_1;

    COUNT_TOTAL_TIME_END
    MPI_Barrier(MPI_COMM_WORLD);
    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Index creation phase completed.\n");
    }
    COUNT_TOTAL_TIME_START

    global_helper_requests = malloc(sizeof(MPI_Request) * comm_sz);

    // PHASE 3: Query Answering
    float *queries_results = NULL;
    switch (query_mode)
    {
    // Classic DRESS Query Answering
    case SEQUENTIAL_QUERY_ANSWERING:
        queries_results = isax_query_binary_file_simple_chatzakis(queries, queries_size, idx_1, minimum_distance, NULL, &exact_search_ParISnew_inmemory_hybrid_ekosmas);
        break;

    // Q-DRESS Query Answering, Baseline {MESSI, WS_ALG}
    case DISTRIBUTED_QUERY_ANSWERING_STATIC:
        queries_results = isax_query_answering_pdr_baseline_chatzakis(queries, queries_size, idx_1, minimum_distance, NULL, &exact_search_ParISnew_inmemory_hybrid_ekosmas, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_STATIC_WS_ALG:
        queries_results = isax_query_answering_pdr_baseline_chatzakis(queries, queries_size, idx_1, minimum_distance, basis_function_filename, &exact_search_workstealing_chatzakis, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;

    // Q-DRESS Query Answering, Dynamic {MESSI, WS_ALG} - No Estimations/Sorting
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC:
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 0, NULL, NULL, MODULE, 0, &exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR:
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 0, NULL, NULL, COORDINATOR, 0, &exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD:
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 0, NULL, NULL, THREAD, 0, &exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_WS_ALG: //!
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 0, NULL, basis_function_filename, MODULE, 0, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_WS_ALG: //!
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 0, NULL, basis_function_filename, COORDINATOR, 0, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD_WS_ALG: //!
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 0, NULL, basis_function_filename, THREAD, 0, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;

    // Q-DRESS Query Answering, Dynamic {MESSI, WS_ALG} - Estimations/Sorting
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_SORTED_QUERIES:
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 1, benchmark_estimations_filename, NULL, MODULE, INITIAL_ESTIMATION, &exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_SORTED_QUERIES:
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 1, benchmark_estimations_filename, NULL, COORDINATOR, INITIAL_ESTIMATION, &exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD_SORTED_QUERIES:
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 1, benchmark_estimations_filename, NULL, THREAD, INITIAL_ESTIMATION, &exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_WS_ALG_SORTED_QUERIES: //!
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 1, benchmark_estimations_filename, basis_function_filename, MODULE, 0, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_WS_ALG_SORTED_QUERIES: //!
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 1, benchmark_estimations_filename, basis_function_filename, COORDINATOR, 0, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD_WS_ALG_SORTED_QUERIES: //!
        queries_results = isax_query_answering_pdr_dynamic_chatzakis(queries, queries_size, idx_1, minimum_distance, 1, benchmark_estimations_filename, basis_function_filename, THREAD, 0, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;

    // Q-DRESS Query Answering, Greedy {MESSI, WS_ALG} - Estimations/Sorting
    case DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY:
        queries_results = isax_query_answering_pdr_greedy_chatzakis(queries, queries_size, idx_1, minimum_distance, benchmark_estimations_filename, NULL, 0, INITIAL_ESTIMATION, &exact_search_ParISnew_inmemory_hybrid_ekosmas, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_SORTED:
        queries_results = isax_query_answering_pdr_greedy_chatzakis(queries, queries_size, idx_1, minimum_distance, benchmark_estimations_filename, NULL, 1, INITIAL_ESTIMATION, &exact_search_ParISnew_inmemory_hybrid_ekosmas, NULL, NULL);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_WS_ALG: //!
        queries_results = isax_query_answering_pdr_greedy_chatzakis(queries, queries_size, idx_1, minimum_distance, benchmark_estimations_filename, basis_function_filename, 0, INITIAL_ESTIMATION, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;
    case DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_SORTED_WS_ALG: //!
        queries_results = isax_query_answering_pdr_greedy_chatzakis(queries, queries_size, idx_1, minimum_distance, benchmark_estimations_filename, basis_function_filename, 1, INITIAL_ESTIMATION, &exact_search_workstealing_chatzakis, NULL, &exact_search_workstealing_helper_single_batch_chatzakis);
        break;

    // Q-DRESS Error Handling
    default:
        printf("[MC] - DRESS: Invalid query mode!\n");
        exit(EXIT_FAILURE);
    }

    // Collect the partial answers from the other nodes and report the final answers
    COUNT_QUERIES_RESULTS_COLLECTION_TIME_START
    COUNT_MPI_TIME_START
    MPI_Request request;
    MPI_Isend(&queries_results[0], queries_size, MPI_FLOAT, MASTER, QUERIES_RESULTS_TAG, MPI_COMM_WORLD, &request);
    COUNT_MPI_TIME_END

    if (my_rank == MASTER)
    {
        recv_final_result(queries_size);
    }

    COUNT_MPI_TIME_START
    MPI_Wait(&request, MPI_STATUS_IGNORE);
    COUNT_MPI_TIME_END

    COUNT_QUERIES_RESULTS_COLLECTION_TIME_END
    COUNT_TOTAL_TIME_END

    process_results_chatzakis(request);

    //collect_queries_times(queries_size);

}
