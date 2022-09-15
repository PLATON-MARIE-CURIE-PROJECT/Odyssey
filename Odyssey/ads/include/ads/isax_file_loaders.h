//
//  isax_file_loaders.h
//  isax
//
//  Created by Kostas Zoumpatianos on 4/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isax_isax_file_loaders_h
#define isax_isax_file_loaders_h
#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "isax_index.h"
#include "isax_query_engine.h"
#include "inmemory_index_engine.h"

#include <mpi.h>

void isax_index_binary_file(const char *ifilename, int ts_num,
                            isax_index *index);
void isax_sorted_index_binary_file(const char *ifilename, int ts_num,
                                   isax_index *index);
void isax_merge_sorted_index_binary_file(const char *ifilename, int ts_num,
                                         isax_index *index);
void isax_query_binary_file(const char *ifilename, int q_num,
                            isax_index *index, float minimum_distance,
                            int min_checked_leaves,
                            query_result (*search_function)(ts_type *, ts_type *, isax_index *, float, int));
void isax_query_binary_file_traditional(const char *ifilename, int q_num, isax_index *index,
                                        float minimum_distance, int min_checked_leaves,
                                        query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float, int));
float *isax_query_binary_file_traditional_ekosmas(const char *ifilename, int q_num, isax_index *index,
                                                  float minimum_distance,
                                                  query_result (*search_function)(search_function_params args));
void isax_query_binary_file_traditional_ekosmas_EP(const char *ifilename, int q_num, isax_index *index,
                                                   float minimum_distance,
                                                   query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float));
void isax_query_binary_file_traditional_ekosmas_lf(const char *ifilename, int q_num, isax_index *index,
                                                   float minimum_distance, const char parallelism_in_subtree, const int third_phase,
                                                   query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float, const char, const int, const int query_id));
void isax_query_binary_file_batch(const char *ifilename, int q_num,
                                  isax_index *index, float minimum_distance,
                                  int min_checked_leaves,
                                  void (*search_function)(ts_type *, ts_type *, isax_index *, float, int, int));
void isax_query_binary_fixbsf_file(const char *ifilename, int q_num, isax_index *index,
                                   float minimum_distance, int min_checked_leaves,
                                   query_result (*search_function)(ts_type *, ts_type *, isax_index *, float, int, float));
void isax_index_baffuer_manager(const char *ifilename, int ts_num, isax_index *index);

void isax_topk_query_binary_file(const char *ifilename, int q_num, isax_index *index,
                                 float minimum_distance, int min_checked_leaves, int k,
                                 pqueue_bsf (*search_function)(ts_type *, ts_type *, isax_index *, float, int, int));
void isax_knn_query_binary_file(const char *ifilename, const char *labelfilename, int q_num, isax_index *index,
                                float minimum_distance, int min_checked_leaves, int k, long int classlength,
                                pqueue_bsf (*search_function)(ts_type *, ts_type *, isax_index *, float, int, int));
void isax_knn_query_binary_file_traditional(const char *ifilename, const char *labelfilename, int q_num, isax_index *index,
                                            float minimum_distance, int min_checked_leaves, int k, long int classlength,
                                            pqueue_bsf (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float, int, int));

// Chatzakis START

// QUERY ANSWERING FINAL FUNCTIONS-MODES
float *isax_query_binary_file_distributed_query_answering_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float));
float *isax_query_binary_file_distributed_makespan_query_answering_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float, communication_module_data *query_data), query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index), dynamic_modes mode, char sort, char *estimations_filename, greedy_alg_comparing_field field_to_cmp);
float *isax_query_binary_file_static_distributed_dress_query_answering_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float), query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index), char *estimations_filename, char sort, greedy_alg_comparing_field field_to_cmp);

float *isax_query_binary_file_simple_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char *basis_function_filename, query_result (*search_function)(search_function_params args));

float *isax_query_binary_file_distributed_workstealing_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance,
                                                                 query_result (*search_function)(int query_id, ts_type *ts, ts_type *paa, isax_index *index,
                                                                                                 node_list *nodelist /*batch_list *batchlist*/, float minimum_distance,
                                                                                                 double (*estimation_func)(double)),
                                                                 char *basis_function_filename,
                                                                 query_result (*ws_search_function)(int query_id, ts_type *ts, ts_type *paa, isax_index *index,
                                                                                                    node_list *nodelist /*batch_list *batchlist*/, float minimum_distance,
                                                                                                    double (*estimation_func)(double), float bsf, int *batches));

/* Algorithms */
void greedy_query_scheduling_chatzakis(dress_query *queries, dress_query *queries_to_ans, int *queries_counter, int q_num, greedy_alg_comparing_field cpm_field);

// DISTRIBUTED DYNAMIC QUERY ANSWERING WITH IDLE COORDINATOR:
int send_queries_module_coordinator_async_chatzakis(int *q_loaded, int q_num, int *process_buffer, MPI_Request *request, int *rec_message, MPI_Request *send_request, int *termination_message_id);
extern int comm_sz;
void send_initial_queries_module_coordinator_async_chatzakis(int *q_loaded, int distributed_queries_initial_burst, int process_buffer_initial[comm_sz][distributed_queries_initial_burst], int *rec_message, MPI_Request *request, MPI_Request *send_request);

// HELPER
void save_query_stats_chatzakis(int q_loaded, query_result result, double time, double *queries_times);

// QUERY LOADERS
ts_type **load_queries_from_file_chatzakis(isax_index *index, int q_num, FILE *ifile);
ts_type **free_queries_chatzakis(isax_index *index, int q_num, ts_type **queries);

void print_query_statistics_results_chatzakis(int q_num);
void inform_my_pair_I_finished_chatzakis();

long int split_series_for_processes_chatzakis(long int dataset_size);
int get_my_pair_rank_chatzakis();

// PRIORITY QUERIES FUNCTIONS
priority_query *load_priority_queries_from_file_chatzakis(isax_index *index, int q_num, FILE *ifile);
void print_priority_queries_chatzakis(priority_query *queries, int q_num);
void merge_query_arr(priority_query *arr, int l, int m, int r);
void merge_sort_priority_queries(priority_query *arr, int l, int r);
void sort_query_array_chatzakis(priority_query *queries, int q_num);

// UTILS
int generate_random_number_chatzakis(int max_num, int min_num);
FILE *fopen_validate_ds_file(const char *ifilename, int q_num, int ts_byte_size);
void shuffle(int *array, size_t n);

typedef struct parallel_query_thread_data
{
    int tid;

    int from;
    int to;

    float *results;
    float minimum_distance;

    isax_index *index;
    ts_type **queries;
    FILE *ifile;
    node_list nodelist;

    query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float, int, int *, int *);

    unsigned long long int for_all_queries_real_dist_counter_perthread;
    unsigned long long int for_all_queries_min_dist_counter_perthread;

    double time_per_thread;

} parallel_query_thread_data;
void *independent_query_answering_chatzakis(void *arg);

// WORKSTEALING
void workstealing(dress_query *queries, int q_num, isax_index *index, float minimum_distance, node_list nodelist,
                  query_result (*ws_search_function)(ws_search_function_params ws_params),
                  double (*estimation_func)(double), float *results, float *shared_bsf_results);

// DRESS QUERIES
dress_query *load_dress_queries_from_file_chatzakis(isax_index *index, int q_num, FILE *ifile, FILE *estimations_file);
void print_dress_queries_chatzakis(dress_query *queries, int q_num);
void merge_dress_query_arr(dress_query *arr, int l, int m, int r, int (*dress_query_cmp)(dress_query, dress_query));
void merge_sort_dress_queries(dress_query *arr, int l, int r, int (*dress_query_cmp)(dress_query, dress_query));
void sort_dress_query_array_chatzakis(dress_query *queries, int q_num, int (*dress_query_cmp)(dress_query, dress_query));
void find_initial_BFSs_dress_queries_chatzakis(dress_query *queries, int q_num, isax_index *index, query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index));
int compare_dress_queries_initBSF(dress_query q1, dress_query q2);
int compare_dress_queries_initEstTime(dress_query q1, dress_query q2);
int compare_dress_queries_initEstTime_revs(dress_query q1, dress_query q2);

void save_subtree_stats(int q_num, int amount);
void free_subtree_array(int q_num);

double (*initialize_basis_function(char *filename))(double);
double sigmoid_parameterized_chatzakis(double x);

// PDR
float *isax_query_answering_pdr_baseline_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char *basis_function_filename,
                                                   query_result (*search_function)(search_function_params args),
                                                   query_result (*ws_search_function)(ws_search_function_params ws_args));

float *isax_query_answering_pdr_dynamic_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char sort, char *estimations_filename, char *basis_function_filename,
                                                  dynamic_modes mode,
                                                  greedy_alg_comparing_field field_to_cmp,
                                                  query_result (*search_function)(search_function_params args),
                                                  query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index),
                                                  query_result (*ws_search_function)(ws_search_function_params ws_args));

float *isax_query_answering_pdr_greedy_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char *estimations_filename, char *basis_function_filename, char sort,
                                                 greedy_alg_comparing_field field_to_cmp,
                                                 query_result (*search_function)(search_function_params args),
                                                 query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index),
                                                 query_result (*ws_search_function)(ws_search_function_params ws_args));

// Chatzakis END

#endif
