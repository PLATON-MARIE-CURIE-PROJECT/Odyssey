//
//  defines.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/19/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
//  Updated by Eleftherios Kosmas on May 2020.
//
#include "config.h"

#ifndef isax_globals_h
#define isax_globals_h

#define STORE_ANSWER

#define ULONG_MAX 0xFFFFFFFFUL

#pragma GCC diagnostic ignored "-Wunused-result"
#pragma GCC diagnostic ignored "-Wunused-variable"

#define PAGE_SIZE 4096
#define PROGRESS_CALCULATE_THREAD_NUMBER 12
#define PROGRESS_FLUSH_THREAD_NUMBER 12
#define QUERIES_THREAD_NUMBER 4
#define DISK_BUFFER_SIZE 8192
#define LOCK_SIZE 65536
///// TYPES /////
typedef unsigned char sax_type;
typedef float ts_type;
typedef unsigned long long file_position_type;
typedef unsigned long long root_mask_type;

enum response
{
    OUT_OF_MEMORY_FAILURE,
    FAILURE,
    SUCCESS
};
enum insertion_mode
{
    PARTIAL = 1,
    TMP = 2,
    FULL = 4,
    NO_TMP = 8
};

enum buffer_cleaning_mode
{
    FULL_CLEAN,
    TMP_ONLY_CLEAN,
    TMP_AND_TS_CLEAN
};
enum node_cleaning_mode
{
    DO_NOT_INCLUDE_CHILDREN = 0,
    INCLUDE_CHILDREN = 1
};

///// DEFINITIONS /////
#define MINVAL -2000000
#define MAXVAL 2000000
#define DELIMITER ' '
#define TRUE 1
#define FALSE 0
#define BUFFER_REALLOCATION_RATE 2

///// GLOBAL VARIABLES /////
int FLUSHES;
unsigned long BYTES_ACCESSED;
float APPROXIMATE;

#define INCREASE_BYTES_ACCESSED(new_bytes) \
    BYTES_ACCESSED += (unsigned long)new_bytes;
#define RESET_BYTES_ACCESSED \
    BYTES_ACCESSED = 0;
#define SET_APPROXIMATE(approximate) \
    APPROXIMATE = approximate;

///// MACROS /////
#define CREATE_MASK(mask, index, sax_array)                                                            \
    int mask__i;                                                                                       \
    for (mask__i = 0; mask__i < index->settings->paa_segments; mask__i++)                              \
        if (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1] & sax_array[mask__i]) \
            mask |= index->settings->bit_masks[index->settings->paa_segments - mask__i - 1];

///// EKOSMAS DEVELOPMENT OUTPUT /////
#ifdef EKOSMAS_DEV_OUTPUT
#define EKOSMAS_PRINT(message) \
    {                          \
        printf(message);       \
        fflush(stdout);        \
    }
#else
#define EKOSMAS_PRINT(message) ;
#endif

///// EKOSMAS CACHE ALIGNMENT /////
#define CACHE_LINE_SIZE 64
#define CACHE_ALIGN __attribute__((aligned(CACHE_LINE_SIZE)))
#define PAD_CACHE(A) ((CACHE_LINE_SIZE - (A % CACHE_LINE_SIZE)) / sizeof(char))

///// EKOSMAS ATOMIC PRIMITIVES /////
#define CASPTR(A, B, C) __sync_bool_compare_and_swap((long *)A, (long)B, (long)C)
// #define CASFLOAT(A, B, C)   __sync_bool_compare_and_swap_4((float *)A, (float)B, (float)C)     // NOT WORKING
#define CASULONG(A, B, C) __sync_bool_compare_and_swap((unsigned long *)A, (unsigned long)B, (unsigned long)C)

///// EKOSMAS ENUM /////
enum
{
    NO_PARALLELISM_IN_SUBTREE,
    BLOCKING_PARALLELISM_IN_SUBTREE,
    LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE,
    LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP,
    LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF,
    LOCKFREE_PARALLELISM_IN_SUBTREE_COW
};

#define MASTER 0
#define DISTS_STATS_NUM 2
#define BSF_STATS_NUM 4

#define QUERIES_RESULTS_TAG 0
#define TIMES_TAG 1
#define DISTS_STATS_TAG 2
#define BSF_STATS_TAG 3
#define QUERIES_TIMES_STATS_TAG 4
#define THREADS_MAX_DIFFERENCE_TAG 5

#define BSF_CLEAN_UP_PHASE_TAG 100

#define WORKSTEALING_END_WITH_MY_INDEX 200
#define WORKSTEALING_I_HAVE_PROCESS_SUBTREES 201

#define WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_OTHER_TREE 2000
#define WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_MY_TREE 10000

#define WORKSTEALING_WORK_ON_MY_TREE 1
#define WORKSTEALING_WORK_ON_OTHER_TREE 2

extern int DEBUG_PRINT_FLAG;
#define DP(exp)           \
    if (DEBUG_PRINT_FLAG) \
    {                     \
        exp               \
    } // Debug print

#include <mpi.h>
MPI_Datatype bsf_msg_type;
struct bsf_msg
{
    int q_num;
    float bsf;
};

#define IS_EVEN(number) (number % 2 == 0)
#define IS_LAST_NODE(rank, comm_sz) (rank == comm_sz - 1)

#define BSF_BLOCK_PER_QUERY 1
#define BSF_DONT_BLOCK_PER_QUERY 2
#define BSF_DONT_SHARE 0

typedef struct query_statistics
{
    float initial_bsf; // the first approximate bsf
    float final_bsf;   // the query answer

    long int real_distances;
    long int lower_bound_distances;

    double total_time; // in microseconds
} query_statistics_t;

typedef enum priority_q
{
    LOW = 0,
    MEDIUM = 1,
    HIGH = 2
} priority_q;

typedef struct priority_query
{
    ts_type *query;
    priority_q priority;
} priority_query;

typedef struct query_data
{
    ts_type *query;
    int id;
    float dist_from_base;
    query_statistics_t stats;
} query_data;

typedef struct query_group
{
    ts_type *query_representative;
    query_data *queries;

    int group_size;

} query_group;

typedef enum greedy_alg_comparing_field
{
    INITIAL_ESTIMATION = 0,
    INITIAL_BSF
} greedy_alg_comparing_field;

typedef enum dynamic_modes
{
    MODULE = 0,
    COORDINATOR,
    THREAD
} dynamic_modes;

typedef struct per_subtree_stats
{
    int height;
    int total_nodes;
    int total_unpruned_nodes;
    int traversal_min_distance_calculations;

    double distance_from_root;
    double BSF_of_tree;
} per_subtree_stats;

typedef struct pdr_node_group
{
    int node_group_id;

    int total_nodes;
    int *nodes;

    int coordinator_node;

    long int total_time_series;
} pdr_node_group;

// simple work stealing, manol
#define SIMPLE_WORK_STEALING_CREATE_MY_INDEX 1
#define SIMPLE_WORK_STEALING_CREATE_OTHER_NODE_INDEX 2

// workstealing, chatzakis
#define SIMPLE_WORK_STEALING_CREATE_OTHER_NODE_INDEX_MULTINODE_PAIR 3

#define SEQUENTIAL_QUERY_ANSWERING 0
//#define PARALLEL_QUERY_ANSWERING 1 // not to be used
#define DISTRIBUTED_QUERY_ANSWERING_STATIC 2
#define INDEX_QUERY_ANSWERING 3
//#define PRIORITY_QUERY_ANSWERING 4
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC 5
//#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_DEBUG 6 // not to be used
#define DYNAMIC_DISTRIBUTED_PRIORITY_QUERY_ANSWERING 7
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR 8
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_SORTED_QUERIES 9
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_SORTED_QUERIES 10
#define DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY 11
#define DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_SORTED 12
#define DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_INITIAL_BSF 13
#define DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_INITIAL_BSF_SORTED 14
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_MODULE_SORTED_INITIAL_BSF 15
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_SORTED_INITIAL_BSF 16
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD 17
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD_SORTED_QUERIES 18
#define WORKSTEALING_SUPPORTED_QUERY_ANSWERING 19

// new ones
#define SEQUENTIAL_QUERY_ANSWERING_WS_ALG 20
#define DISTRIBUTED_QUERY_ANSWERING_STATIC_WS_ALG 21
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_WS_ALG 22
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_WS_ALG 23
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD_WS_ALG 24
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_WS_ALG_SORTED_QUERIES 25
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_WS_ALG_SORTED_QUERIES 26
#define DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD_WS_ALG_SORTED_QUERIES 27
#define DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_WS_ALG 28
#define DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_SORTED_WS_ALG 29

#define DISTRIBUTED_QUERIES_REQUEST_QUERY 800
#define DISTRIBUTED_QUERIES_SEND_QUERY 801

#define WORKSTEALING_INFORM_AVAILABILITY 900
#define WORKSTEALING_QUERY_ANSWERING_COMPLETION 901
#define WORKSTEALING_DATA_SEND 902
#define WORKSTEALING_BSF_SHARE 903

#define DISTRIBUTED_QUERY_ANSWERING_STATIC_PARTIAL 20

#define IS_RUNNING_DISTRIBUTED_QUERIES                                                      \
    (query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC ||                                    \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC ||                                   \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR ||                       \
     query_mode == DYNAMIC_DISTRIBUTED_PRIORITY_QUERY_ANSWERING ||                          \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_SORTED_QUERIES ||                    \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_SORTED_QUERIES ||        \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY ||                             \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_SORTED ||                      \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_INITIAL_BSF ||                 \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_GREEDY_INITIAL_BSF_SORTED ||          \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_MODULE_SORTED_INITIAL_BSF ||         \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_SORTED_INITIAL_BSF ||    \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD ||                \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR_THREAD_SORTED_QUERIES || \
     query_mode == WORKSTEALING_SUPPORTED_QUERY_ANSWERING ||                                \
     query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_PARTIAL)

#define IS_RUNNING_PARTIAL_REP_DISTRIBUTED_QUERIES \
    (query_mode == DISTRIBUTED_QUERY_ANSWERING_STATIC_PARTIAL)

#define CLASSIC_DRESS_QA (query_mode == SEQUENTIAL_QUERY_ANSWERING)

#define FIND_NODE_GROUP(rank, nodes_per_group) rank / nodes_per_group
#define IS_LAST_NODE_OF_GROUP(rank, nodes_per_group) (node_groups[FIND_NODE_GROUP(rank, nodes_per_group)].nodes[nodes_per_group - 1] == rank)
#define FIND_COORDINATOR_NODE_RANK(rank, nodes_per_group) (node_groups[FIND_NODE_GROUP(rank, nodes_per_group)].coordinator_node)

#define CALL_MODULE (comm_data->module_func)(comm_data->q_loaded, comm_data->q_num, comm_data->process_buffer, comm_data->request, comm_data->rec_message, comm_data->send_request, comm_data->termination_message_id);
#define DYNAMIC_TERMINATION_MESSAGE -1

#define MAX_PQs_WORKSTEALING 20000

#define ENABLE_PRINTS_WORKSTEALING_HELPER 0 //?
#define ENABLE_PRINTS_WORKSTEALING_INTERNALS 0 //?

#define ENABLE_PRINTS_BSF_SHARING 0
#define ENABLE_PRINTS_PER_QUERY_RESULT 0
#define ENABLE_PRINTS_WORKSTEALING 0

///// BENCHMARKING /////
#ifdef BENCHMARK
#include <time.h>
#include <sys/time.h>

double tS;
double tE;
int added_tree_node;
// --------------------------------------
struct timeval mpi_time_start;                  // manol
struct timeval queries_result_collection_start; // manol

struct timeval manol_tmp_start; // manol

struct timeval total_time_start;
struct timeval queue_time_start;
struct timeval parse_time_start;
struct timeval input_time_start;
struct timeval input2_time_start;
struct timeval cal_time_start;
struct timeval output_time_start;
struct timeval output2_time_start;
struct timeval load_node_start;
struct timeval current_time;
struct timeval fetch_start;
struct timeval fetch_check_start;
struct timeval fill_rec_bufs_time_start;
struct timeval create_tree_index_time_start;
struct timeval query_answering_time_start;
struct timeval queue_fill_time_start;
struct timeval queue_fill_help_time_start;
struct timeval queue_process_time_start;
struct timeval queue_process_help_time_start;
struct timeval initialize_index_tree_time_start;

struct timeval initialize_index_tree_time_start;

/* Chatzakis */
struct timeval communication_time_start;
struct timeval query_preprocessing_time_start;
struct timeval pq_preprocessing_time_start;
struct timeval pq_fill_helping_time_start;
struct timeval workstealing_qa_time_start;

double total_input_time;
double total_queue_time;
double total_input2_time;
double total_cal_time;
double load_node_time;
double total_output_time;
double total_output2_time;
double total_parse_time;
double total_time;

double mpi_time; // manol
double queries_result_collection_time;
double manol_tmp_time;

/*Chatzakis Globals*/
double communication_time;
double query_preprocessing_time;
double pq_preprocessing_time;
double pq_fill_helping_time;
double workstealing_qa_time;

int total_tree_nodes;
int loaded_nodes;
int checked_nodes;
file_position_type loaded_records;
unsigned long int LBDcalculationnumber;
unsigned long int RDcalculationnumber;
unsigned long blocks_helped;
unsigned long block_help_avoided;
unsigned long subtrees_helped;
unsigned long subtree_help_avoided;

double fill_rec_bufs_time;
double create_tree_index_time;
double query_answering_time;
double queue_fill_time;
double queue_fill_help_time;
double queue_process_time;
double queue_process_help_time;
double initialize_index_tree_time;
long int ts_in_RecBufs_cnt;
long int ts_in_tree_cnt;
long int non_empty_subtrees_cnt;
long int min_ts_in_subtrees;
long int max_ts_in_subtrees;
char DO_NOT_HELP;
#define INIT_STATS()                    \
    total_input_time = 0;               \
    total_output_time = 0;              \
    total_time = 0;                     \
    total_parse_time = 0;               \
    total_tree_nodes = 0;               \
    loaded_nodes = 0;                   \
    checked_nodes = 0;                  \
    load_node_time = 0;                 \
    loaded_records = 0;                 \
    APPROXIMATE = 0;                    \
    BYTES_ACCESSED = 0;                 \
    blocks_helped = 0;                  \
    block_help_avoided = 0;             \
    subtrees_helped = 0;                \
    subtree_help_avoided = 0;           \
    fill_rec_bufs_time = 0;             \
    create_tree_index_time = 0;         \
    query_answering_time = 0;           \
    queue_fill_time = 0;                \
    queue_fill_help_time = 0;           \
    queue_process_time = 0;             \
    queue_process_help_time = 0;        \
    initialize_index_tree_time = 0;     \
    mpi_time = 0;                       \
    queries_result_collection_time = 0; \
    communication_time = 0;             \
    query_preprocessing_time = 0;       \
    pq_preprocessing_time = 0;          \
    pq_fill_helping_time = 0;           \
    workstealing_qa_time = 0;

// printf("input\t output\t nodes\t checked_nodes\t bytes_accessed\t loaded_nodes\t loaded_records\t approximate_distance\t distance\t total\n");
#define PRINT_STATS(result_distance) printf("%lf\t %lf\t %d\t %d\t %ld\t %d\t %lld\t %lf\t %lf\t %lf\t %d\t %lf\t %lf\n", \
                                            total_input_time, total_output_time,                                          \
                                            total_tree_nodes, checked_nodes,                                              \
                                            BYTES_ACCESSED, loaded_nodes,                                                 \
                                            loaded_records, APPROXIMATE,                                                  \
                                            result_distance, total_time,                                                  \
                                            block_help_avoided, fill_rec_bufs_time, create_tree_index_time);
//#define PRINT_STATS(result_distance) printf("%d\t",loaded_nodes);

#define PRINT_QUERY_STATS(result_distance) printf("%d\t %d\t %ld\t %d\t %lld\t %lf\t %lf\t\n",  \
                                                  total_tree_nodes, checked_nodes,              \
                                                  BYTES_ACCESSED, loaded_nodes, loaded_records, \
                                                  APPROXIMATE, result_distance);

#define MY_PRINT_STATS(result_distance) printf("%lf\t %lf\t %lf\t %lf\t %lf\t %lf\t  %lu\t %lu\t %lu\t %lu\t %ld\t %ld\t %ld\t %ld\t %ld\t %ld\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\n", \
                                               total_input_time, total_output_time,                                                                                                          \
                                               total_time, fill_rec_bufs_time, create_tree_index_time, query_answering_time,                                                                 \
                                               blocks_helped, block_help_avoided, subtrees_helped, subtree_help_avoided,                                                                     \
                                               ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - dataset_size, ts_in_tree_cnt - ts_in_RecBufs_cnt,                                                      \
                                               non_empty_subtrees_cnt, min_ts_in_subtrees, max_ts_in_subtrees, (double)ts_in_tree_cnt / non_empty_subtrees_cnt,                              \
                                               queue_fill_time, queue_fill_help_time, queue_process_time, queue_process_help_time, initialize_index_tree_time,                               \
                                               queue_fill_time + queue_process_time + initialize_index_tree_time);

#define PRINT_STATS_TO_FILE(result_distance, fp, process_rank) fprintf(fp, "%d\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %lu\t %lu\t %lu\t %lu\t %ld\t %ld\t %ld\t %ld\t %ld\t %ld\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\t %lf\n", \
                                                                       process_rank, total_input_time, total_output_time,                                                                                                    \
                                                                       total_time, fill_rec_bufs_time, create_tree_index_time, query_answering_time,                                                                         \
                                                                       blocks_helped, block_help_avoided, subtrees_helped, subtree_help_avoided,                                                                             \
                                                                       ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - dataset_size, ts_in_tree_cnt - ts_in_RecBufs_cnt,                                                              \
                                                                       non_empty_subtrees_cnt, min_ts_in_subtrees, max_ts_in_subtrees, (double)ts_in_tree_cnt / non_empty_subtrees_cnt,                                      \
                                                                       queue_fill_time, queue_fill_help_time, queue_process_time, queue_process_help_time, initialize_index_tree_time,                                       \
                                                                       queue_fill_time + queue_process_time + initialize_index_tree_time);

#define min(x, y) (x < y ? x : y)
#define COUNT_NEW_NODE() __sync_fetch_and_add(&total_tree_nodes, 1);
#define COUNT_BLOCKS_HELPED(num) __sync_fetch_and_add(&blocks_helped, num);
#define COUNT_BLOCK_HELP_AVOIDED(num) __sync_fetch_and_add(&block_help_avoided, num);
#define COUNT_SUBTREES_HELPED(num) __sync_fetch_and_add(&subtrees_helped, num);
#define COUNT_SUBTREE_HELP_AVOIDED(num) __sync_fetch_and_add(&subtree_help_avoided, num);
#define COUNT_LOADED_NODE() loaded_nodes++;
#define COUNT_CHECKED_NODE() checked_nodes++;

#define COUNT_LOADED_RECORD() loaded_records++;

#define COUNT_INPUT_TIME_START gettimeofday(&input_time_start, NULL);
#define COUNT_FILL_REC_BUF_TIME_START gettimeofday(&fill_rec_bufs_time_start, NULL);
#define COUNT_CREATE_TREE_INDEX_TIME_START gettimeofday(&create_tree_index_time_start, NULL);
#define COUNT_QUERY_ANSWERING_TIME_START gettimeofday(&query_answering_time_start, NULL);
#define COUNT_QUEUE_FILL_TIME_START gettimeofday(&queue_fill_time_start, NULL);
#define COUNT_QUEUE_FILL_HELP_TIME_START gettimeofday(&queue_fill_help_time_start, NULL);
#define COUNT_QUEUE_PROCESS_TIME_START gettimeofday(&queue_process_time_start, NULL);
#define COUNT_QUEUE_PROCESS_HELP_TIME_START gettimeofday(&queue_process_help_time_start, NULL);
#define COUNT_INITIALIZE_INDEX_TREE_TIME_START gettimeofday(&initialize_index_tree_time_start, NULL);
#define COUNT_QUEUE_TIME_START gettimeofday(&queue_time_start, NULL);
#define COUNT_CAL_TIME_START gettimeofday(&cal_time_start, NULL);
#define COUNT_INPUT2_TIME_START gettimeofday(&input2_time_start, NULL);
#define COUNT_OUTPUT_TIME_START gettimeofday(&output_time_start, NULL);
#define COUNT_OUTPUT2_TIME_START gettimeofday(&output2_time_start, NULL);
#define COUNT_TOTAL_TIME_START gettimeofday(&total_time_start, NULL);

#define COUNT_MPI_TIME_START gettimeofday(&mpi_time_start, NULL);
#define COUNT_QUERIES_RESULTS_COLLECTION_TIME_START gettimeofday(&queries_result_collection_start, NULL);
#define COUNT_MANOL_TMP_TIME_START gettimeofday(&manol_tmp_start, NULL);

#define COUNT_PARSE_TIME_START gettimeofday(&parse_time_start, NULL);
#define COUNT_LOAD_NODE_START gettimeofday(&load_node_start, NULL);
#define COUNT_INPUT_TIME_END                                             \
    gettimeofday(&current_time, NULL);                                   \
    tS = input_time_start.tv_sec * 1000000 + (input_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);         \
    total_input_time += (tE - tS);
#define COUNT_INPUT2_TIME_END                                              \
    gettimeofday(&current_time, NULL);                                     \
    tS = input2_time_start.tv_sec * 1000000 + (input2_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);           \
    total_input2_time += (tE - tS);
#define COUNT_CAL_TIME_END                                           \
    gettimeofday(&current_time, NULL);                               \
    tS = cal_time_start.tv_sec * 1000000 + (cal_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);     \
    total_cal_time += (tE - tS);
#define COUNT_OUTPUT_TIME_END                                              \
    gettimeofday(&current_time, NULL);                                     \
    tS = output_time_start.tv_sec * 1000000 + (output_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);           \
    total_output_time += (tE - tS);
#define COUNT_OUTPUT2_TIME_END                                               \
    gettimeofday(&current_time, NULL);                                       \
    tS = output2_time_start.tv_sec * 1000000 + (output2_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);             \
    total_output2_time += (tE - tS);
#define COUNT_TOTAL_TIME_END                                             \
    gettimeofday(&current_time, NULL);                                   \
    tS = total_time_start.tv_sec * 1000000 + (total_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);         \
    total_time += (tE - tS);

#define COUNT_MPI_TIME_END                                           \
    gettimeofday(&current_time, NULL);                               \
    tS = mpi_time_start.tv_sec * 1000000 + (mpi_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);     \
    mpi_time += (tE - tS);

#define COUNT_QUERIES_RESULTS_COLLECTION_TIME_END                                                      \
    gettimeofday(&current_time, NULL);                                                                 \
    tS = queries_result_collection_start.tv_sec * 1000000 + (queries_result_collection_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                                       \
    queries_result_collection_time += (tE - tS);

// einai xronos se Microseconds
#define COUNT_MANOL_TMP_TIME_END                                       \
    gettimeofday(&current_time, NULL);                                 \
    tS = manol_tmp_start.tv_sec * 1000000 + (manol_tmp_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);       \
    manol_tmp_time += (tE - tS);

#define COUNT_PARSE_TIME_END                                             \
    gettimeofday(&current_time, NULL);                                   \
    tS = parse_time_start.tv_sec * 1000000 + (parse_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);         \
    total_parse_time += (tE - tS);
#define COUNT_LOAD_NODE_END                                            \
    gettimeofday(&current_time, NULL);                                 \
    tS = load_node_start.tv_sec * 1000000 + (load_node_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);       \
    load_node_time += (tE - tS);
#define COUNT_QUEUE_TIME_END                                             \
    gettimeofday(&current_time, NULL);                                   \
    tS = queue_time_start.tv_sec * 1000000 + (queue_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);         \
    total_queue_time += (tE - tS);
#define COUNT_FILL_REC_BUF_TIME_END                                                      \
    gettimeofday(&current_time, NULL);                                                   \
    tS = fill_rec_bufs_time_start.tv_sec * 1000000 + (fill_rec_bufs_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                         \
    fill_rec_bufs_time += (tE - tS);
#define COUNT_CREATE_TREE_INDEX_TIME_END                                                         \
    gettimeofday(&current_time, NULL);                                                           \
    tS = create_tree_index_time_start.tv_sec * 1000000 + (create_tree_index_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                                 \
    create_tree_index_time += (tE - tS);
#define COUNT_QUERY_ANSWERING_TIME_END                                                       \
    gettimeofday(&current_time, NULL);                                                       \
    tS = query_answering_time_start.tv_sec * 1000000 + (query_answering_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                             \
    query_answering_time += (tE - tS);
#define COUNT_QUEUE_FILL_TIME_END                                                  \
    gettimeofday(&current_time, NULL);                                             \
    tS = queue_fill_time_start.tv_sec * 1000000 + (queue_fill_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                   \
    queue_fill_time += (tE - tS);
#define COUNT_QUEUE_FILL_HELP_TIME_END                                                       \
    gettimeofday(&current_time, NULL);                                                       \
    tS = queue_fill_help_time_start.tv_sec * 1000000 + (queue_fill_help_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                             \
    queue_fill_help_time += (tE - tS);
#define COUNT_QUEUE_PROCESS_TIME_END                                                     \
    gettimeofday(&current_time, NULL);                                                   \
    tS = queue_process_time_start.tv_sec * 1000000 + (queue_process_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                         \
    queue_process_time += (tE - tS);
#define COUNT_QUEUE_PROCESS_HELP_TIME_END                                                          \
    gettimeofday(&current_time, NULL);                                                             \
    tS = queue_process_help_time_start.tv_sec * 1000000 + (queue_process_help_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                                   \
    queue_process_help_time += (tE - tS);
#define COUNT_INITIALIZE_INDEX_TREE_TIME_END                                                             \
    gettimeofday(&current_time, NULL);                                                                   \
    tS = initialize_index_tree_time_start.tv_sec * 1000000 + (initialize_index_tree_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                                         \
    initialize_index_tree_time += (tE - tS);

/* Chatzakis Macros */
#define COUNT_COMMUNICATION_TIME_START gettimeofday(&communication_time_start, NULL);
#define COUNT_COMMUNICATION_TIME_END                                                     \
    gettimeofday(&current_time, NULL);                                                   \
    tS = communication_time_start.tv_sec * 1000000 + (communication_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                         \
    communication_time += (tE - tS);

#define COUNT_QUERY_PREPROCESSING_TIME_START gettimeofday(&query_preprocessing_time_start, NULL);
#define COUNT_QUERY_PREPROCESSING_TIME_END                                                           \
    gettimeofday(&current_time, NULL);                                                               \
    tS = query_preprocessing_time_start.tv_sec * 1000000 + (query_preprocessing_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                                     \
    query_preprocessing_time += (tE - tS);

#define COUNT_PQ_FILL_HELP_TIME_START gettimeofday(&pq_fill_helping_time_start, NULL);
#define COUNT_PQ_FILL_HELP_TIME_END                                                          \
    gettimeofday(&current_time, NULL);                                                       \
    tS = pq_fill_helping_time_start.tv_sec * 1000000 + (pq_fill_helping_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                             \
    pq_fill_helping_time += (tE - tS);

#define COUNT_QA_WORKSTEALING_TIME_START gettimeofday(&workstealing_qa_time_start, NULL);
#define COUNT_QA_WORKSTEALING_TIME_END                                                       \
    gettimeofday(&current_time, NULL);                                                       \
    tS = workstealing_qa_time_start.tv_sec * 1000000 + (workstealing_qa_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                             \
    workstealing_qa_time += (tE - tS);

#define COUNT_PQ_PREPROCESSING_TIME_START gettimeofday(&pq_preprocessing_time_start, NULL);
#define COUNT_PQ_PREPROCESSING_TIME_END                                                        \
    gettimeofday(&current_time, NULL);                                                         \
    tS = pq_preprocessing_time_start.tv_sec * 1000000 + (pq_preprocessing_time_start.tv_usec); \
    tE = current_time.tv_sec * 1000000 + (current_time.tv_usec);                               \
    pq_preprocessing_time += (tE - tS);

#else
#define INIT_STATS() ;
#define PRINT_STATS() ;
#define PRINT_QUERY_STATS() ;
#define MY_PRINT_STATS() ;
#define PRINT_STATS_TO_FILE(result_distance, fp) ;
#define COUNT_NEW_NODE() ;
#define COUNT_BLOCK_HELP_AVOIDED(num) ;
#define COUNT_SUBTREE_HELP_AVOIDED(num) ;
#define COUNT_CHECKED_NODE() ;
#define COUNT_LOADED_NODE() ;
#define COUNT_LOADED_RECORD() ;
#define COUNT_INPUT_TIME_START ;
#define COUNT_INPUT_TIME_END ;
#define COUNT_OUTPUT_TIME_START ;
#define COUNT_OUTPUT_TIME_END ;
#define COUNT_TOTAL_TIME_START ;
#define COUNT_TOTAL_TIME_END ;
#define COUNT_PARSE_TIME_START ;
#define COUNT_PARSE_TIME_END ;
#define COUNT_LOAD_NODE_END ;
#define COUNT_QUEUE_TIME_END ;
#define COUNT_FILL_REC_BUF_TIME_START ;
#define COUNT_FILL_REC_BUF_TIME_END ;
#define COUNT_CREATE_TREE_INDEX_TIME_START ;
#define COUNT_CREATE_TREE_INDEX_TIME_END ;
#define COUNT_QUERY_ANSWERING_TIME_START ;
#define COUNT_QUERY_ANSWERING_TIME_END ;
#endif
#endif