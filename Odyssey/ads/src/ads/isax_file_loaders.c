//
//  isax_file_loaders.c
//  isax
//
//  Created by Kostas Zoumpatianos on 4/7/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <float.h>
#include <unistd.h>
#include <math.h>
#include <assert.h>

#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_query_engine.h"
#include "ads/isax_node_record.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_first_buffer_layer.h"

#include "ads/inmemory_index_engine.h"

#include <mpi.h>

// Botao's version
void isax_query_binary_file_traditional(const char *ifilename, int q_num, isax_index *index, float minimum_distance, int min_checked_leaves, query_result (*search_function)(ts_type *, ts_type *, isax_index *, node_list *, float, int))
{
    COUNT_INPUT_TIME_START
    fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);
    file_position_type total_records = sz / index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num)
    {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    int q_loaded = 0;
    ts_type *ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_QUERY_ANSWERING_TIME_START
    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);
    // sax_type * sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;
    // EKOSMAS: The first_node does not contain correct value in LockFreeMESSI (it is not used, i.e. the doubly linked list has been removed)
    isax_node *current_root_node = index->first_node;

    // EKOSMAS: maintain root nodes into an array, so that it is possible to execute FAI
    while (1)
    {
        if (current_root_node != NULL)
        {
            nodelist.nlist[nodelist.node_amount] = current_root_node;
            // EKOSMAS: The next pointer does not contain correct value in LockFreeMESSI (it is not used, i.e. the doubly linked list has been removed)
            current_root_node = current_root_node->next;
            nodelist.node_amount++;
        }
        else
        {
            break;
        }
    }
    // printf("the node node_amount is %d\n",nodelist.node_amount ); fflush(stdout);

    while (q_loaded < q_num)
    {
        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        COUNT_INPUT_TIME_START
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        COUNT_INPUT_TIME_END
        // printf("Querying for: %d\n", index->settings->ts_byte_size * q_loaded);

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START

        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments,
                    index->settings->ts_values_per_paa_segment);
        query_result result = search_function(ts, paa, index, &nodelist, minimum_distance, min_checked_leaves);

        fflush(stdout);
#if VERBOSE_LEVEL >= 1
        printf("[%p]: Distance: %lf\n", result.node, result.distance);
#endif
        // sax_from_paa(paa, sax, index->settings->paa_segments, index->settings->sax_alphabet_cardinality, index->settings->sax_bit_cardinality);
        // if (index->settings->timeseries_size * sizeof(ts_type) * q_loaded == 1024) {
        //     sax_print(sax, index->settings->paa_segments, index->settings->sax_bit_cardinality);
        // }

        q_loaded++;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        PRINT_QUERY_STATS(result.distance)

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }

    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    COUNT_INPUT_TIME_START
    free(ts);
    fclose(ifile);
    fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END
}

// manolis
extern int share_with_nodes_bsf;
extern int my_rank;
extern int comm_sz;

extern MPI_Comm *communicators;
extern MPI_Request *requests;
extern struct bsf_msg *shared_bsfs;
extern int *bcasts_per_query;

float tmp_value = -1.0;

extern double *queries_times;

extern query_statistics_t *queries_statistics_manol;

void blocking_share_bsf_clean_up_for_next_query(int q_loaded);
extern int simple_work_stealing;

extern unsigned long long int total_real_distance_counter; // total distances from all the threads for a specific query
extern unsigned long long int total_min_distance_counter;

extern unsigned long long int for_all_queries_real_dist_counter;
extern unsigned long long int for_all_queries_min_dist_counter;

extern isax_index *idx_1; // create tree index for the node data
extern isax_index *idx_2; // create tree index for the other node (workstealing)

extern float *raw_file_1;
extern float *raw_file_2;
extern float *tmp_raw_file;

const int PROCESSED_SUBTREES = WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_OTHER_TREE;

extern int time_series_size;
extern int distributed_queries_initial_burst;
extern int per_node_queries_greedy_alg;

extern double sum_of_per_query_times;
extern double per_node_score_greedy_alg;

extern per_subtree_stats **subtree_stats;
extern char gather_subtree_stats;
extern FILE *subtree_stats_file;
extern char *subtree_stats_filename;

extern int workstealing_chatzakis;

// Sigmoid Func params
double SIG_M = 0;
double SIG_m = 0;
double SIG_b = 0;
double SIG_c = 0;
double SIG_d = 0;

extern int batches_to_send;

extern long int node_groups_number;
extern pdr_node_group *node_groups;
extern long int node_group_total_nodes;
extern int bsf_sharing_pdr;

query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_simple_workstealing_manol(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist, float minimum_distance, int node_counter_fetch_add_start_value, int workstealing_round, int specify_working_tree, int compute_initial_bsf);

query_result workOnMyTreeIndex(ts_type *ts, ts_type *paa, int subtrees, float minimum_distance, node_list *nodelist, int queryNum)
{
    query_result result;
    int compute_init_bsf_flag = 1;
    // i work on my tree, but partially

    result.distance = minimum_distance;

    int node_counter_fetch_add_start_value = 0;
    int workstealing_round = 1;

    int ready;
    MPI_Request request;
    int rec_message;

    // work on my own tree index
    while (1)
    {
        // check if the other node has work-steal already from my tree
        // do it iteratevly because may be process already multiple subtrees,
        // so i can receive multiple message
        // because every time that process a chunk of the sub trees send a message
        while (1)
        {
            MPI_Irecv(&rec_message, 1, MPI_INT, /*my_rank == 0 ? 1 : 0*/ get_my_pair_rank_chatzakis(), WORKSTEALING_I_HAVE_PROCESS_SUBTREES, MPI_COMM_WORLD, &request);
            // The corresponding send has not been issued yet, this MPI_Test will "fail".
            MPI_Test(&request, &ready, MPI_STATUS_IGNORE);
            if (ready)
            {
                // [MC] -- What????
                // idle node work steal sub trees from the end of the tree, so i reduce the sub trees that need to be processed
                subtrees -= WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_OTHER_TREE;
            }
            else
            {
                // if there is no message continue to work on the other chunk of the sub trees
                break;
            }
        }
        // my tree is already processed (from me and from the other node(work-steal))
        if (node_counter_fetch_add_start_value > subtrees)
        {
            break;
        }

        // process part of subtrees i.e., 10.000
        query_result tmp_result = exact_search_ParISnew_inmemory_hybrid_ekosmas_simple_workstealing_manol(ts, paa, idx_1, nodelist,
                                                                                                          result.distance, node_counter_fetch_add_start_value, workstealing_round, WORKSTEALING_WORK_ON_MY_TREE, compute_init_bsf_flag);

        if (compute_init_bsf_flag)
        {
            queries_statistics_manol[queryNum].initial_bsf = tmp_result.init_bsf;
        }
        compute_init_bsf_flag = 0; // change this flag, so the bsf will calculated only once for a query
        if (tmp_result.distance < result.distance)
        {
            result.distance = tmp_result.distance; // update the best finded result so far
        }

        node_counter_fetch_add_start_value += WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_MY_TREE;
        workstealing_round++;
    }

    return result;
}

void workStealOnOtherNodeIndex(ts_type *ts, ts_type *paa, node_list *nodelist, query_result *result)
{
    // I WORKSTEAL FROM OTHER NODE TREE
    int node_counter_fetch_add_start_value;

    // printf("Node %d begun workstealing...\n", my_rank);

    tmp_raw_file = raw_file_2; // change the raw data that i work on (for the calculation of real distances)

    // i must get the ack that also the other node has finished, if i know that the other node has finished i can skip the workstealing process
    int received;
    MPI_Request recv_request;
    MPI_Irecv(&received, 1, MPI_INT, /*my_rank == 0 ? 1 : 0*/ get_my_pair_rank_chatzakis(), WORKSTEALING_END_WITH_MY_INDEX, MPI_COMM_WORLD, &recv_request);

    node_counter_fetch_add_start_value = 0;
    int workstealing_round = 1;
    while (1)
    {
        int ready;
        MPI_Test(&recv_request, &ready, MPI_STATUS_IGNORE);
        if (ready)
        { // i received the ack from the other node that have finished as well, so i quit the work-stealing
            break;
        }

        // do not need to work steal other sub trees so wait until i received thatthe other node has finished as well
        if (node_counter_fetch_add_start_value + WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_OTHER_TREE > nodelist->node_amount)
        {
            continue;
        }

        // inform the other node that i will process some subtrees (from the end)
        MPI_Request send_request;
        MPI_Isend(&PROCESSED_SUBTREES, 1, MPI_INT, /*my_rank == 0 ? 1 : 0*/ get_my_pair_rank_chatzakis(), WORKSTEALING_I_HAVE_PROCESS_SUBTREES, MPI_COMM_WORLD, &send_request);

        query_result tmp_result = exact_search_ParISnew_inmemory_hybrid_ekosmas_simple_workstealing_manol(ts, paa, idx_2, nodelist, result->distance, node_counter_fetch_add_start_value, workstealing_round, WORKSTEALING_WORK_ON_OTHER_TREE, 0);

        if (tmp_result.distance < result->distance)
        { // keep the minimun founded distance
            result->distance = tmp_result.distance;
        }

        node_counter_fetch_add_start_value += WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_OTHER_TREE;
        workstealing_round++;
    }
    tmp_raw_file = raw_file_1; // end of the processing of the query, so update the raw_data to point to the correct data series (for real distance calculations)
}

void inform_other_node_that_i_finished()
{
    int buffer_sent = 0;
    MPI_Request send_request;
    // inform the other node that i have finished with my query processing
    MPI_Isend(&buffer_sent, 1, MPI_INT, my_rank == 0 ? 1 : 0, WORKSTEALING_END_WITH_MY_INDEX, MPI_COMM_WORLD, &send_request);
}

// ekosmas version
float *isax_query_binary_file_traditional_ekosmas(const char *ifilename, int q_num, isax_index *index, float minimum_distance, query_result (*search_function)(/*ts_type *, ts_type *, isax_index *, node_list *, float)*/ search_function_params args))
{
    COUNT_INPUT_TIME_START
    if (comm_sz == 1)
        fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);

    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);
    file_position_type total_records = sz / index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num)
    {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    int q_loaded = 0;
    ts_type *ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    // Independent queries
    // ts_type * ts2 = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    COUNT_INPUT_TIME_END
    // printf("[MC] - DEBUG - INPUT TIME END: After reading the queries calculations: %f\n", total_input_time);

    COUNT_OUTPUT_TIME_START
    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Query Answering with %d threads.\n", maxquerythread);
    }

    COUNT_QUERY_ANSWERING_TIME_START

    // struct timeval t1, t2;
    // double elapsedTime;
    //  start timer
    // gettimeofday(&t1, NULL);

    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++)
    {
        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        if (!current_fbl_node->node)
        {
            printf("Error: node is NULL!\t");
            fflush(stdout);
            getchar();
        }
        nodelist.node_amount++;
    }

    if (gather_subtree_stats)
    {
        subtree_stats = (per_subtree_stats **)malloc(sizeof(per_subtree_stats *) * q_num);
        for (int i = 0; i < q_num; i++)
        {
            subtree_stats[i] = (per_subtree_stats *)malloc(sizeof(per_subtree_stats) * nodelist.node_amount);
        }
    }
    printf("[MC] --- --- --- All right with allocation! --- --- --- \n");

    // workstealing - for other node
    node_list nodelist_2;
    if (simple_work_stealing)
    {
        nodelist_2.nlist = malloc(sizeof(isax_node *) * pow(2, idx_2->settings->paa_segments));
        nodelist_2.node_amount = 0;

        // maintain root nodes into an array, so that it is possible to execute FAI
        for (int j = 0; j < idx_2->fbl->number_of_buffers; j++)
        {

            parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(idx_2->fbl))->soft_buffers[j];
            if (!current_fbl_node->initialized)
            {
                continue;
            }

            nodelist_2.nlist[nodelist_2.node_amount] = current_fbl_node->node;
            if (!current_fbl_node->node)
            {
                printf("Error: node is NULL!!!!\t");
                fflush(stdout);
                getchar();
            }
            nodelist_2.node_amount++;
        }
    }

    double prev_query_answering_time = 0;
    float *results = malloc(sizeof(float) * q_num);
    search_function_params args;
    while (q_loaded < q_num)
    {
        prev_query_answering_time = query_answering_time;
        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        // take this time

        COUNT_INPUT_TIME_START

        // Independent queries, KEEP ONLY LINE FREAD ts
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        // fread(ts2, sizeof(ts_type),index->settings->timeseries_size,ifile);
        // if(my_rank == 0){
        //     ts = ts;
        // }
        // if(my_rank == 1){
        //     ts = ts2;
        // }

        /*printf("Q[%d]: ", q_loaded);
        for (int y = 0; y < index->settings->timeseries_size; y++)
        {
            printf("%f, ", ts[y]);
        }
        printf("\n");*/

        COUNT_INPUT_TIME_END

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START

        // Parse ts and make PAA representation
        paa_from_ts(ts, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

        args.ts = ts;
        args.paa = paa;
        args.index = index;
        args.nodelist = &nodelist;
        args.minimum_distance = minimum_distance;

        query_result result;
        if (!simple_work_stealing)
        {
            // printf("[MC] - No work stealing, performing a simple search! ... \n");
            result = search_function(args);
        }
        else
        {
            result = workOnMyTreeIndex(ts, paa, nodelist.node_amount, minimum_distance, &nodelist, q_loaded);
            // inform_other_node_that_i_finished(); [MC] removed
            inform_my_pair_I_finished_chatzakis();
            // workStealOnOtherNodeIndex(ts, paa, nodelist_2, &result); //change here from MC
            workStealOnOtherNodeIndex(ts, paa, &nodelist_2, &result); // change here from MC
        }

        // printf("[MC] -- A result just obtained using search function: [Distance, Init_bsf] = [%f, %f]\n", result.distance, result.init_bsf);

        // manolis
        results[q_loaded] = result.distance;
        q_loaded++;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END

        // not needed for Independent queries
        MPI_Barrier(MPI_COMM_WORLD);
        // move the query answering time end here !

        // update the statistics
        double time_for_the_query = query_answering_time - prev_query_answering_time;
        queries_times[q_loaded - 1] = time_for_the_query;
        printf("[MC] - Node %d, query %d, result: %f, time: %f\n", my_rank, q_loaded - 1, results[q_loaded - 1], time_for_the_query / 1000000);

        queries_statistics_manol[q_loaded - 1].total_time = time_for_the_query; // query statistics
        queries_statistics_manol[q_loaded - 1].final_bsf = result.distance;     // query statistics

        queries_statistics_manol[q_loaded - 1].lower_bound_distances = total_min_distance_counter;
        queries_statistics_manol[q_loaded - 1].real_distances = total_real_distance_counter;

        for_all_queries_real_dist_counter += total_real_distance_counter;
        for_all_queries_min_dist_counter += total_min_distance_counter;

        total_real_distance_counter = 0; // reset the counter for the next query
        total_min_distance_counter = 0;

        // CLEAN UP FOR THE NEXT QUERY
        // COUNT_TOTAL_TIME_END
        // blocking_share_bsf_clean_up_for_next_query(q_loaded); // else we can use differents communicators for each query
        // COUNT_TOTAL_TIME_START

        if (comm_sz == 1)
            PRINT_QUERY_STATS(result.distance);
        // Ekosmas: Manolis
        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }

    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    // stop timer
    // gettimeofday(&t2, NULL);

    // compute and print the elapsed time in millisec
    // elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;    // sec to ms
    // elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    // printf("QA SINGLE THREAD %f ms.\n", elapsedTime / 1000.0);

    COUNT_OUTPUT_TIME_END

    COUNT_INPUT_TIME_START
    free(ts);
    fclose(ifile);
    if (comm_sz == 1)
        fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END

    if (gather_subtree_stats)
    {
        save_subtree_stats(q_num, nodelist.node_amount);
        // free_subtree_array(q_num);
    }

    return results;
    // return version1(ifilename, q_num, index, minimum_distance, search_function);
}

// this function just consume all the message before continue to the next query
// just for cleaning the MPI buffers, so on the next query you receive message that is related to that query
void blocking_share_bsf_clean_up_for_next_query(int q_loaded)
{
    if (share_with_nodes_bsf == BSF_DONT_BLOCK_PER_QUERY)
    {
        for (int i = 0; i < comm_sz; ++i)
        {
            requests[i] = MPI_REQUEST_NULL; // is safe because all the requests is NULL_REQUEST, else the program will hang on MPI_Wait
            shared_bsfs[i].q_num = -1;
            shared_bsfs[i].bsf = -1.0;
            bcasts_per_query[i] = 0;
        }
    }

    if (share_with_nodes_bsf != BSF_BLOCK_PER_QUERY)
    {
        return;
    }

    // else must block and collect/clean up the message from MPI
    MPI_Barrier(MPI_COMM_WORLD); // Prin kanei start tous timers prepei na einai oles oi processes sto idio simeio

    MPI_Request local_reqs[comm_sz];
    for (int i = 0; i < comm_sz; ++i)
    {

        local_reqs[i] = MPI_REQUEST_NULL;
        if (i == my_rank)
        {
            continue;
        }
        MPI_Isend(&bcasts_per_query[my_rank], 1, MPI_INT, i, BSF_CLEAN_UP_PHASE_TAG, MPI_COMM_WORLD, &local_reqs[i]); // inform other processes for the number of my write bcast (bcast that i send)
    }

    int total_bcasts_num = 0;
    for (int i = 0; i < comm_sz; ++i)
    {

        if (my_rank == i)
        {
            MPI_Wait(&requests[i], MPI_STATUS_IGNORE);
            MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]); // send/write the final bcast
        }
        else
        {
            MPI_Recv(&total_bcasts_num, 1, MPI_INT, i, BSF_CLEAN_UP_PHASE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // den exo labei osa bcast exei kanei o allos
            if (bcasts_per_query[i] < total_bcasts_num)
            {
                DP(printf("[%d] q-%d!!!!EDOOOO!!!!!\n", my_rank, q_loaded - 1);)
                while (bcasts_per_query[i] < total_bcasts_num)
                {
                    MPI_Wait(&requests[i], MPI_STATUS_IGNORE);
                    bcasts_per_query[i] += 1;
                    MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]); // on the final while iteration it listen/open an extra bcast (receive the final broadcast)
                }
            }
        }

        MPI_Wait(&requests[i], MPI_STATUS_IGNORE);
    }

    for (int i = 0; i < comm_sz; ++i)
    {
        if (i == my_rank)
        {
            continue;
        }
        MPI_Wait(&local_reqs[i], MPI_STATUS_IGNORE); // messages sended successfully
    }

    for (int i = 0; i < comm_sz; ++i)
    {
        MPI_Wait(&requests[i], MPI_STATUS_IGNORE);
        // printf("{%d} wait on %d ok!\n", my_rank, i); fflush(stdout);
    }

    for (int i = 0; i < comm_sz; ++i)
    {
        requests[i] = MPI_REQUEST_NULL; // is safe because all the requests is NULL_REQUEST, else the program will hang on MPI_Wait
        shared_bsfs[i].q_num = -1;
        shared_bsfs[i].bsf = -1.0;
        bcasts_per_query[i] = 0;
    }
    // TODO: NA DO TI PREPEI NA KANO INITIALIZE

    MPI_Barrier(MPI_COMM_WORLD); // Prin kanei start tous timers prepei na einai oles oi processes sto idio simeio
}

/*
    --------------------------------------------------------------
    --------------------------------------------------------------
    --------------------------------------------------------------

    --------------------------------------------------------------
    --------------------------------------------------------------
    ------------------- Chatzakis Thesis Stuff -------------------
    --------------------------------------------------------------
    --------------------------------------------------------------

    --------------------------------------------------------------
    --------------------------------------------------------------
    --------------------------------------------------------------

*/

/* SIMPLE Query Answering */
extern int gather_init_bsf_only;
extern char *init_bsf_filename;
FILE *init_bsfs_fp;
float *isax_query_binary_file_simple_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char *basis_function_filename, query_result (*search_function)(search_function_params args))
{
    COUNT_INPUT_TIME_START
    FILE *ifile = fopen_validate_ds_file(ifilename, q_num, index->settings->ts_byte_size);
    dress_query *queries = load_dress_queries_from_file_chatzakis(index, q_num, ifile, NULL);
    double (*basis_func)(double) = NULL;
    if (basis_function_filename != NULL)
    {
        basis_func = initialize_basis_function(basis_function_filename);
    }

    if (gather_init_bsf_only)
    {
        init_bsfs_fp = fopen(init_bsf_filename, "w+");
    }

    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START

    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Distributed Query Answering with %d threads.\n", maxquerythread);
    }

    COUNT_QUERY_ANSWERING_TIME_START

    float *results = malloc(sizeof(float) * q_num);
    float *shared_bsf_results = malloc(sizeof(float) * q_num);
    for (int i = 0; i < q_num; i++)
    {
        results[i] = FLT_MAX;
        shared_bsf_results[i] = FLT_MAX;
    }

    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);
    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++)
    {
        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        if (!current_fbl_node->node)
        {
            printf("Error: node is NULL!\t");
            fflush(stdout);
            getchar();
        }
        nodelist.node_amount++;
    }

    // printf("[MC] - Node amount %d\n", nodelist.node_amount);

    search_function_params args;
    double prev_query_answering_time = 0;
    for (int i = 0; i < q_num; i++)
    {
        prev_query_answering_time = query_answering_time;

        paa_from_ts(queries[i].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

        args.query_id = i;
        args.ts = queries[i].query;
        args.paa = paa;
        args.index = index;
        args.nodelist = &nodelist;
        args.minimum_distance = minimum_distance;
        args.estimation_func = basis_func;
        args.shared_bsf_results = shared_bsf_results;
        args.comm_data = NULL;

        query_result result = search_function(args);
        results[i] = result.distance;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END
        double time_for_the_query = query_answering_time - prev_query_answering_time;
        save_query_stats_chatzakis(i, result, time_for_the_query, queries_times);
        
        //auto pou epsanxa
        printf("[MC] - Node %d, query %d, result: %f, time: %f\n", my_rank, i, results[i], time_for_the_query / 1000000);
        //fprintf(init_bsfs_fp, "%f\n", results[i]);

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }

    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    MPI_Barrier(MPI_COMM_WORLD);

    COUNT_INPUT_TIME_START
    fclose(ifile);
    if (comm_sz == 1)
        fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END

    if (gather_init_bsf_only)
    {
        fclose(init_bsfs_fp);
    }

    return results;
}

/* PARTIAL DATA REPLICATION MODULAR CODE */
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
float *isax_query_answering_pdr_baseline_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char *basis_function_filename,
                                                   query_result (*search_function)(search_function_params args),
                                                   query_result (*ws_search_function)(ws_search_function_params ws_args))
{
    COUNT_INPUT_TIME_START
    FILE *ifile = fopen_validate_ds_file(ifilename, q_num, index->settings->ts_byte_size);
    dress_query *queries = load_dress_queries_from_file_chatzakis(index, q_num, ifile, NULL);

    double (*basis_func)(double) = NULL;
    if (basis_function_filename != NULL)
    {
        basis_func = initialize_basis_function(basis_function_filename);
    }

    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START

    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Distributed Query Answering with %d threads.\n", maxquerythread);
    }

    COUNT_QUERY_ANSWERING_TIME_START

    COUNT_QUERY_PREPROCESSING_TIME_START
    int sets, from, to;

    sets = q_num / node_group_total_nodes;

    from = (my_rank - FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes)) * sets;

    if (IS_LAST_NODE_OF_GROUP(my_rank, node_group_total_nodes))
    {
        to = q_num;
    }
    else
    {
        to = ((my_rank - FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes)) + 1) * sets; // is ok?
    }

    // printf("[MC] -- Node %d StaticQA [from=%d,to=%d]\n", my_rank, from, to);

    COUNT_QUERY_PREPROCESSING_TIME_END

    float *results = malloc(sizeof(float) * q_num);
    float *shared_bsf_results = malloc(sizeof(float) * q_num);
    for (int i = 0; i < q_num; i++)
    {
        results[i] = FLT_MAX;
        shared_bsf_results[i] = FLT_MAX;
    }

    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);
    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++)
    {
        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        if (!current_fbl_node->node)
        {
            printf("Error: node is NULL!\t");
            fflush(stdout);
            getchar();
        }
        nodelist.node_amount++;
    }

    // printf("[MC] - Node amount %d\n", nodelist.node_amount);

    double prev_query_answering_time = 0;
    search_function_params params;
    for (int i = from; i < to; i++)
    {
        prev_query_answering_time = query_answering_time;

        // Parse ts and make PAA representation
        paa_from_ts(queries[i].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

        params.ts = queries[i].query;
        params.paa = paa;
        params.index = index;
        params.nodelist = &nodelist;
        params.query_id = i;
        params.minimum_distance = minimum_distance;
        params.estimation_func = basis_func;
        params.comm_data = NULL;
        params.shared_bsf_results = shared_bsf_results;

        query_result result = search_function(params);

        results[i] = result.distance;

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END
        double time_for_the_query = query_answering_time - prev_query_answering_time;
        save_query_stats_chatzakis(i, result, time_for_the_query, queries_times);
        // printf("[MC] - Node: %d, query: %d, result: %f, time: %f, pqs: %d, pqs_stolen: %d\n", my_rank, i, results[i], time_for_the_query / 1000000, result.total_pqs, result.stolen_pqs);

        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }

    COUNT_QA_WORKSTEALING_TIME_START
    if (workstealing_chatzakis)
    {
        workstealing(queries, q_num, index, minimum_distance, nodelist, ws_search_function, basis_func, results, shared_bsf_results);
    }
    COUNT_QA_WORKSTEALING_TIME_END

    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    MPI_Barrier(MPI_COMM_WORLD);

    COUNT_INPUT_TIME_START
    fclose(ifile);
    if (comm_sz == 1)
        fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END

    return results;
}

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
                                                  query_result (*ws_search_function)(ws_search_function_params ws_args))
{
    COUNT_INPUT_TIME_START

    int q_loaded = 0;
    FILE *ifile = fopen_validate_ds_file(ifilename, q_num, index->settings->ts_byte_size);
    FILE *estimations_file = NULL; // NULL is accepted as filename.
    if (estimations_filename != NULL)
    {
        estimations_file = fopen(estimations_filename, "r");
        if (estimations_file == NULL)
        {
            fprintf(stderr, "File %s not found!\n", estimations_filename);
            exit(-1);
        };
    }
    dress_query *queries = load_dress_queries_from_file_chatzakis(index, q_num, ifile, estimations_file);

    double (*basis_func)(double) = NULL;
    if (basis_function_filename != NULL)
    {
        basis_func = initialize_basis_function(basis_function_filename);
    }

    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START

    if (my_rank == MASTER)
    {
        printf("[MC] - Master: PDR-Dynamic Distributed Query Answering with %d threads.\n", maxquerythread);
    }

    COUNT_QUERY_ANSWERING_TIME_START

    COUNT_QUERY_PREPROCESSING_TIME_START
    if (sort)
    {
        if (field_to_cmp == INITIAL_BSF)
        {
            find_initial_BFSs_dress_queries_chatzakis(queries, q_num, index, approximate_search_func);
            sort_dress_query_array_chatzakis(queries, q_num, compare_dress_queries_initBSF);
        }
        else if (field_to_cmp == INITIAL_ESTIMATION)
        {
            sort_dress_query_array_chatzakis(queries, q_num, compare_dress_queries_initEstTime);
        }
    }
    COUNT_QUERY_PREPROCESSING_TIME_END

    float *results = malloc(sizeof(float) * q_num);
    float *shared_bsf_results = malloc(sizeof(float) * q_num);
    for (int i = 0; i < q_num; i++)
    {
        results[i] = FLT_MAX;
        shared_bsf_results[i] = FLT_MAX;
    }

    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++)
    {
        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        if (!current_fbl_node->node)
        {
            printf("Error: node is NULL!\t");
            fflush(stdout);
            getchar();
        }
        nodelist.node_amount++;
    }

    // printf("[MC] -- Node amount to process from proccess %d: %d\n", my_rank, nodelist.node_amount);

    double prev_query_answering_time = 0;

    MPI_Request request[comm_sz /*node_group_total_nodes*/];      //!
    MPI_Request send_request[comm_sz /*node_group_total_nodes*/]; //!

    int process_buffer_initial[comm_sz /*node_group_total_nodes*/][distributed_queries_initial_burst]; //!
    int process_buffer[comm_sz /*node_group_total_nodes*/];                                            //!

    int ready;
    int rec_message;
    int buffer_sent = 0;
    int termination_message_id = -1;

    communication_module_data comm_data;
    search_function_params args;
    if (my_rank == FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes))
    {
        send_initial_queries_module_coordinator_async_chatzakis(&q_loaded, distributed_queries_initial_burst, process_buffer_initial, &rec_message, request, send_request);
    }

    while (1)
    {
        if (my_rank == FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes))
        {
            if (mode == MODULE || mode == THREAD)
            {
                prev_query_answering_time = query_answering_time;

                int query_to_keep_stats = q_loaded; // because q_loaded num may change inside search_function
                q_loaded++;                         // this is patch because this one could be increased when new query request comes inside the qa of master coordinator.

                comm_data.module_func = &send_queries_module_coordinator_async_chatzakis;
                comm_data.q_loaded = &q_loaded; // holds the query id to be sent.
                comm_data.rec_message = &rec_message;
                comm_data.termination_message_id = &termination_message_id;
                comm_data.q_num = q_num;
                comm_data.request = request;
                comm_data.send_request = send_request;
                comm_data.process_buffer = process_buffer;
                comm_data.mode = mode;

                paa_from_ts(queries[query_to_keep_stats].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

                args.query_id = query_to_keep_stats;
                args.ts = queries[query_to_keep_stats].query;
                args.paa = paa;
                args.index = index;
                args.nodelist = &nodelist;
                args.minimum_distance = minimum_distance;
                args.comm_data = &comm_data;
                args.estimation_func = basis_func;
                args.shared_bsf_results = shared_bsf_results;

                query_result result = search_function(args);
                results[query_to_keep_stats] = result.distance;

                COUNT_QUERY_ANSWERING_TIME_END
                COUNT_OUTPUT_TIME_END
                double time_for_the_query = query_answering_time - prev_query_answering_time;
                if (ENABLE_PRINTS_PER_QUERY_RESULT)
                {

                    if (workstealing_chatzakis)
                    {
                        printf("[MC] - Node: %d, query: %d, result: %f, time: %f, pqs: %d, pqs_stolen: %d\n", my_rank, query_to_keep_stats, results[query_to_keep_stats], time_for_the_query / 1000000, result.total_pqs, result.stolen_pqs);
                    }
                    else
                    {

                        printf("[MC] - Node %d answered the query %d with result %f in %f s.\n", my_rank, query_to_keep_stats, results[query_to_keep_stats], time_for_the_query / 1000000);
                    }
                }
                save_query_stats_chatzakis(query_to_keep_stats, result, time_for_the_query, queries_times);
                COUNT_OUTPUT_TIME_START
                COUNT_QUERY_ANSWERING_TIME_START
            }

            if (!send_queries_module_coordinator_async_chatzakis(&q_loaded, q_num, process_buffer, request, &rec_message, send_request, &termination_message_id))
            {
                break;
            }
        }
        else
        {
            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Recv(&q_loaded, 1, MPI_INT, FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes), DISTRIBUTED_QUERIES_SEND_QUERY, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END

            // printf("[MC] - Node %d recieved the query %d from MASTER\n", my_rank, q_loaded);

            if (q_loaded >= q_num || q_loaded == -1)
            {
                break;
            }

            prev_query_answering_time = query_answering_time;

            paa_from_ts(queries[q_loaded].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

            args.query_id = q_loaded;
            args.ts = queries[q_loaded].query;
            args.paa = paa;
            args.index = index;
            args.nodelist = &nodelist;
            args.minimum_distance = minimum_distance;
            args.comm_data = NULL;
            args.estimation_func = basis_func;
            args.shared_bsf_results = shared_bsf_results;

            query_result result = search_function(args);

            results[q_loaded] = result.distance;

            COUNT_QUERY_ANSWERING_TIME_END
            COUNT_OUTPUT_TIME_END
            double time_for_the_query = query_answering_time - prev_query_answering_time;
            save_query_stats_chatzakis(q_loaded, result, time_for_the_query, queries_times);
            if (ENABLE_PRINTS_PER_QUERY_RESULT)
            {
                if (workstealing_chatzakis)
                {
                    printf("[MC] - Node: %d, query: %d, result: %f, time: %f, pqs: %d, pqs_stolen: %d\n", my_rank, q_loaded, results[q_loaded], time_for_the_query / 1000000, result.total_pqs, result.stolen_pqs);
                }
                else
                {

                    printf("[MC] - Node %d answered the query %d with result %f in %f s.\n", my_rank, q_loaded, results[q_loaded], time_for_the_query / 1000000);
                }
            }
            COUNT_OUTPUT_TIME_START
            COUNT_QUERY_ANSWERING_TIME_START

            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Isend(&buffer_sent, 1, MPI_INT, FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes), DISTRIBUTED_QUERIES_REQUEST_QUERY, MPI_COMM_WORLD, &send_request[FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes)]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END

            // printf("[MC] - Node %d requested new query from MASTER node.\n", my_rank);
        }
    }

    COUNT_QA_WORKSTEALING_TIME_START
    if (workstealing_chatzakis)
    {
        workstealing(queries, q_num, index, minimum_distance, nodelist, ws_search_function, basis_func, results, shared_bsf_results);
    }
    COUNT_QA_WORKSTEALING_TIME_END

    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    MPI_Barrier(MPI_COMM_WORLD); // let all nodes finish this phase together (but not count the times based on this)

    COUNT_INPUT_TIME_START
    fclose(ifile);
    if (comm_sz == 1)
        fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END

    return results;
}

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
float *isax_query_answering_pdr_greedy_chatzakis(const char *ifilename, int q_num, isax_index *index, float minimum_distance, char *estimations_filename, char *basis_function_filename, char sort,
                                                 greedy_alg_comparing_field field_to_cmp,
                                                 query_result (*search_function)(search_function_params args),
                                                 query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index),
                                                 query_result (*ws_search_function)(ws_search_function_params ws_args))
{
    COUNT_INPUT_TIME_START
    int q_loaded = 0;
    FILE *ifile = fopen_validate_ds_file(ifilename, q_num, index->settings->ts_byte_size);
    FILE *estimations_file = NULL;
    if (estimations_filename != NULL)
    {
        estimations_file = fopen(estimations_filename, "r");
        if (estimations_file == NULL)
        {
            fprintf(stderr, "File %s not found!\n", estimations_filename);
            exit(-1);
        };
    }
    dress_query *queries = load_dress_queries_from_file_chatzakis(index, q_num, ifile, estimations_file);

    double (*basis_func)(double) = NULL;
    if (basis_function_filename != NULL)
    {
        basis_func = initialize_basis_function(basis_function_filename);
    }

    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START

    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Static Sorted Query Answering With %d Threads.\n", maxquerythread);
    }

    COUNT_QUERY_ANSWERING_TIME_START
    COUNT_QUERY_PREPROCESSING_TIME_START

    if (field_to_cmp == INITIAL_BSF)
    {
        find_initial_BFSs_dress_queries_chatzakis(queries, q_num, index, approximate_search_func);
    }

    if (sort)
    {
        if (field_to_cmp == INITIAL_ESTIMATION)
        {
            sort_dress_query_array_chatzakis(queries, q_num, compare_dress_queries_initEstTime);
        }
        else if (field_to_cmp == INITIAL_BSF)
        {
            sort_dress_query_array_chatzakis(queries, q_num, compare_dress_queries_initBSF);
        }
    }

    if (my_rank == MASTER)
    {
        // print_dress_queries_chatzakis(queries, q_num);
    }

    dress_query queries_to_ans[q_num]; // The worst case for a node is q_num - 1, but this allocation covers everything.
    int queries_counter = 0;
    greedy_query_scheduling_chatzakis(queries, queries_to_ans, &queries_counter, q_num, field_to_cmp);    
    COUNT_QUERY_PREPROCESSING_TIME_END

    float *results = malloc(sizeof(float) * q_num);
    float *shared_bsf_results = malloc(sizeof(float) * q_num);
    for (int i = 0; i < q_num; i++)
    {
        results[i] = FLT_MAX;
        shared_bsf_results[i] = FLT_MAX;
    }

    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node *) * pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++)
    {
        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        if (!current_fbl_node->node)
        {
            printf("Error: node is NULL!\t");
            fflush(stdout);
            getchar();
        }
        nodelist.node_amount++;
    }

    double prev_query_answering_time = 0;
    search_function_params args;
    for (int curr_query = 0; curr_query < queries_counter; curr_query++)
    {
        prev_query_answering_time = query_answering_time;
        paa_from_ts(queries_to_ans[curr_query].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

        args.ts = queries_to_ans[curr_query].query;
        args.paa = paa;
        args.index = index;
        args.minimum_distance = minimum_distance;
        args.query_id = curr_query;
        args.nodelist = &nodelist;
        args.estimation_func = basis_func;
        args.comm_data = NULL;
        args.shared_bsf_results = shared_bsf_results;

        query_result result = search_function(args);
        results[queries_to_ans[curr_query].id] = result.distance;

        //printf("[MC] - Node %d answering for query %d, result: %f\n", my_rank, queries_to_ans[curr_query].id, result.distance);

        COUNT_QUERY_ANSWERING_TIME_END
        COUNT_OUTPUT_TIME_END
        double time_for_the_query = query_answering_time - prev_query_answering_time;
        save_query_stats_chatzakis(queries_to_ans[curr_query].id, result, time_for_the_query, queries_times);
        COUNT_OUTPUT_TIME_START
        COUNT_QUERY_ANSWERING_TIME_START
    }

    COUNT_QA_WORKSTEALING_TIME_START
    if (workstealing_chatzakis)
    {
        workstealing(queries, q_num, index, minimum_distance, nodelist, ws_search_function, basis_func, results, shared_bsf_results);
    }
    COUNT_QA_WORKSTEALING_TIME_END

    free(nodelist.nlist);
    free(paa);

    COUNT_QUERY_ANSWERING_TIME_END
    COUNT_OUTPUT_TIME_END

    MPI_Barrier(MPI_COMM_WORLD);

    COUNT_INPUT_TIME_START
    fclose(ifile);
    if (comm_sz == 1)
        fprintf(stderr, ">>> Finished querying.\n");
    COUNT_INPUT_TIME_END

    return results;
}

/* QUERY PROCESSING METHODS */
ts_type **load_queries_from_file_chatzakis(isax_index *index, int q_num, FILE *ifile)
{
    fseek(ifile, 0L, SEEK_SET);
    ts_type **queries = (ts_type **)malloc(sizeof(ts_type *) * q_num);
    for (int i = 0; i < q_num; i++)
    {
        queries[i] = (ts_type *)malloc(sizeof(ts_type) * index->settings->timeseries_size);
    }

    // printf("Queries initialized.\n");

    for (int i = 0; i < q_num; i++)
    {
        fread(queries[i], sizeof(ts_type), index->settings->timeseries_size, ifile);
    }

    fseek(ifile, 0L, SEEK_SET);

    return queries;
}

ts_type **free_queries_chatzakis(isax_index *index, int q_num, ts_type **queries)
{
    for (int i = 0; i < q_num; i++)
    {
        free(queries[i]);
        queries[i] = NULL;
    }

    free(queries);
    queries = NULL; // this has no effect due to by-value passing

    return NULL;
}

void print_query_statistics_results_chatzakis(int q_num)
{
    for (int i = 0; i < q_num; i++)
    {
        printf("[%d]: %f %f %f\n", i, queries_statistics_manol[i].initial_bsf, queries_statistics_manol[i].final_bsf, queries_statistics_manol[i].total_time / 1000);
    }
}

int get_my_pair_rank_chatzakis()
{
    return (my_rank % 2 == 0) ? my_rank + 1 : my_rank - 1;
}

void inform_my_pair_I_finished_chatzakis()
{
    int buffer_sent = 0;
    MPI_Request send_request;
    // inform the other node that i have finished with my query processing
    MPI_Isend(&buffer_sent, 1, MPI_INT, /*my_rank == 0 ? 1 : 0*/ get_my_pair_rank_chatzakis(), WORKSTEALING_END_WITH_MY_INDEX, MPI_COMM_WORLD, &send_request);
}

/* Algorithms */
void greedy_query_scheduling_chatzakis(dress_query *queries, dress_query *queries_to_ans, int *queries_counter, int q_num, greedy_alg_comparing_field cmp_field)
{
    int coordinator_of_current_group_rank = FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes);
    int current_rank = coordinator_of_current_group_rank;

    double current_max_score = 0;

    double node_score[comm_sz];

    for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        node_score[rank] = 0.0;
    }

    for (int curr_query = 0; curr_query < q_num; curr_query++)
    {
        double curr_score_to_add = 0;
        if (cmp_field == INITIAL_ESTIMATION)
        {
            curr_score_to_add = queries[curr_query].initial_estimation;
        }
        else if (cmp_field == INITIAL_BSF)
        {
            curr_score_to_add = queries[curr_query].initialBSF.distance;
        }

        node_score[current_rank] += curr_score_to_add;

        if (my_rank == current_rank)
        {
            queries_to_ans[(*queries_counter)++] = queries[curr_query];
        }

        if (node_score[current_rank] >= current_max_score)
        {
            current_max_score = node_score[current_rank];
            current_rank = coordinator_of_current_group_rank + (current_rank + 1) % node_group_total_nodes; //!
        }
    }

    // printf("Node %d has %f score and will answer %d queries.\n", my_rank, node_score[my_rank], *queries_counter); // DEBUG check

    per_node_score_greedy_alg = node_score[my_rank];
    per_node_queries_greedy_alg = *queries_counter;
}

// DRESS QUERIES METHODS
dress_query *load_dress_queries_from_file_chatzakis(isax_index *index, int q_num, FILE *ifile, FILE *estimations_file)
{
    fseek(ifile, 0L, SEEK_SET);
    if (estimations_file != NULL)
    {
        fseek(estimations_file, 0L, SEEK_SET);
    }

    dress_query *queries = (dress_query *)malloc(sizeof(dress_query) * q_num);

    for (int i = 0; i < q_num; i++)
    {
        queries[i].query = (ts_type *)malloc(sizeof(ts_type) * index->settings->timeseries_size);

        fread(queries[i].query, sizeof(ts_type), index->settings->timeseries_size, ifile);

        if (estimations_file != NULL)
        {
            // fread(&queries[i].initial_estimation, sizeof(double), 1, estimations_file);
            fscanf(estimations_file, "%lf", &queries[i].initial_estimation);
            // printf("[MC] - Estimation of query %d is %f\n", i, queries[i].initial_estimation);
        }

        queries[i].id = i;
        queries[i].priority = HIGH;

        queries[i].stats.final_bsf = FLT_MAX;
        queries[i].stats.initial_bsf = FLT_MAX;
        queries[i].stats.lower_bound_distances = 0;
        queries[i].stats.real_distances = 0;
        queries[i].stats.total_time = 0;
    }

    fseek(ifile, 0L, SEEK_SET);
    if (estimations_file != NULL)
    {
        fseek(estimations_file, 0L, SEEK_SET);
    }

    return queries;
}

void print_dress_queries_chatzakis(dress_query *queries, int q_num)
{
    printf("[MC] - DRESS Queries --- \n");
    char *arr[3] = {"LOW",
                    "MEDIUM",
                    "HIGH"};

    for (int i = 0; i < q_num; i++)
    {
        if (queries[i].query == NULL)
        {
            printf("[MC] - PQueries: Something allocated wrong!\n");
            exit(-1);
        }

        printf("%f\n", queries[i].initial_estimation);
    }

    return;
}

void merge_dress_query_arr(dress_query *arr, int l, int m, int r, int (*dress_query_cmp)(dress_query, dress_query))
{
    int i, j, k;
    int n1 = m - l + 1;
    int n2 = r - m;

    /* create temp arrays */
    dress_query L[n1], R[n2];

    /* Copy data to temp arrays L[] and R[] */
    for (i = 0; i < n1; i++)
        L[i] = arr[l + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[m + 1 + j];

    i = 0; // Initial index of first subarray
    j = 0; // Initial index of second subarray
    k = l; // Initial index of merged subarray
    while (i < n1 && j < n2)
    {
        if (dress_query_cmp(L[i], R[j]))
        {
            arr[k] = L[i];
            i++;
        }
        else
        {
            arr[k] = R[j];
            j++;
        }
        k++;
    }

    while (i < n1)
    {
        arr[k] = L[i];
        i++;
        k++;
    }

    while (j < n2)
    {
        arr[k] = R[j];
        j++;
        k++;
    }
}

void merge_sort_dress_queries(dress_query *arr, int l, int r, int (*dress_query_cmp)(dress_query, dress_query))
{
    if (l < r)
    {
        int m = l + (r - l) / 2;

        merge_sort_dress_queries(arr, l, m, dress_query_cmp);
        merge_sort_dress_queries(arr, m + 1, r, dress_query_cmp);

        merge_dress_query_arr(arr, l, m, r, dress_query_cmp);
    }
}

void sort_dress_query_array_chatzakis(dress_query *queries, int q_num, int (*dress_query_cmp)(dress_query, dress_query))
{
    // printf("[MC] - DRESS Queries Before Sort:\n");
    // print_dress_queries_chatzakis(queries, q_num);

    merge_sort_dress_queries(queries, 0, q_num - 1, dress_query_cmp);

    // printf("[MC] - DRESS Queries After Sort:\n");
    //if(my_rank==MASTER)
    //    print_dress_queries_chatzakis(queries, q_num);
}

int compare_dress_queries_initBSF(dress_query q1, dress_query q2)
{
    return (q1.stats.initial_bsf > q2.stats.initial_bsf);
}

int compare_dress_queries_initEstTime(dress_query q1, dress_query q2)
{
    return (q1.initial_estimation > q2.initial_estimation);
}

int compare_dress_queries_initEstTime_revs(dress_query q1, dress_query q2)
{
    return !(q1.initial_estimation > q2.initial_estimation);
}

void find_initial_BFSs_dress_queries_chatzakis(dress_query *queries, int q_num, isax_index *index, query_result (*approximate_search_func)(ts_type *, ts_type *, isax_index *index))
{
    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    for (int i = 0; i < q_num; i++)
    {
        paa_from_ts(queries[i].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);
        query_result bsf_result = approximate_search_func(queries[i].query, paa, index);

        queries[i].initialBSF = bsf_result;
        queries[i].stats.initial_bsf = bsf_result.distance;
    }
}

/* MPI COMMUNICATION METHODS */
void send_initial_queries_module_coordinator_async_chatzakis(int *q_loaded, int distributed_queries_initial_burst, int process_buffer_initial[comm_sz][distributed_queries_initial_burst], int *rec_message, MPI_Request *request, MPI_Request *send_request)
{

    int coordinator_of_current_group_rank = FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes);
    for (int i = 0; i < distributed_queries_initial_burst; i++)
    {
        for (int rank = coordinator_of_current_group_rank + 1; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
        {
            process_buffer_initial[rank][i] = (*q_loaded)++; //! ADD PRINTS
        }
    }

    for (int i = 0; i < distributed_queries_initial_burst; i++)
    {
        for (int rank = coordinator_of_current_group_rank + 1; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
        {
            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Isend(&process_buffer_initial[rank][i], 1, MPI_INT, rank, DISTRIBUTED_QUERIES_SEND_QUERY, MPI_COMM_WORLD, &send_request[rank]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END

            // printf("[MC] - Master send to node %d the query %d to answer.\n", rank, process_buffer_initial[rank][i]);
        }
    }

    for (int rank = coordinator_of_current_group_rank + 1; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        COUNT_COMMUNICATION_TIME_START
        COUNT_MPI_TIME_START
        MPI_Irecv(rec_message, 1, MPI_INT, rank, DISTRIBUTED_QUERIES_REQUEST_QUERY, MPI_COMM_WORLD, &request[rank]);
        COUNT_MPI_TIME_END
        COUNT_COMMUNICATION_TIME_END
    }
}

int send_queries_module_coordinator_async_chatzakis(int *q_loaded, int q_num, int *process_buffer, MPI_Request *request, int *rec_message, MPI_Request *send_request, int *termination_message_id)
{
    // printf(" ===== Module Started: RANK %d=====\n", my_rank);
    // April Patch Notes: process_buffer not used, local buffer implemented. Same fot termination message.
    // int termination_message_local = DYNAMIC_TERMINATION_MESSAGE;
    int ready;
    int coordinator_of_current_group_rank = FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes);

    for (int rank = coordinator_of_current_group_rank + 1; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        COUNT_COMMUNICATION_TIME_START
        COUNT_MPI_TIME_START
        MPI_Test(&request[rank], &ready, MPI_STATUS_IGNORE);
        COUNT_MPI_TIME_END
        COUNT_COMMUNICATION_TIME_END

        if (ready)
        {
            // moved this wait call here because I am going to change the buffer.
            MPI_Wait(&send_request[rank], MPI_STATUS_IGNORE);
            process_buffer[rank] = (*q_loaded)++;

            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Irecv(rec_message, 1, MPI_INT, rank, DISTRIBUTED_QUERIES_REQUEST_QUERY, MPI_COMM_WORLD, &request[rank]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END
        }
        else
        {
            process_buffer[rank] = -1;
        }
    }

    for (int rank = coordinator_of_current_group_rank + 1; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        if (process_buffer[rank] >= 0)
        {
            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Isend(&process_buffer[rank], 1, MPI_INT, rank, DISTRIBUTED_QUERIES_SEND_QUERY, MPI_COMM_WORLD, &send_request[rank]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END

            // printf("[MC] - Master send to node %d the query %d to answer.\n", rank, process_buffer[rank]);
        }
    }

    // printf(" ===== Module Ended =====\n");

    if ((*q_loaded) >= q_num)
    {
        for (int rank = coordinator_of_current_group_rank + 1; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
        {
            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Isend(termination_message_id, 1, MPI_INT, rank, DISTRIBUTED_QUERIES_SEND_QUERY, MPI_COMM_WORLD, &send_request[rank]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END
        }

        return 0;
    }

    return 1;
}

/* WORKSTEALING */
void workstealing(dress_query *queries, int q_num, isax_index *index, float minimum_distance, node_list nodelist,
                  query_result (*ws_search_function)(ws_search_function_params ws_params),
                  double (*estimation_func)(double), float *results, float *shared_bsf_results)
{
    ts_type *paa = malloc(sizeof(ts_type) * index->settings->paa_segments);

    int term_message = DYNAMIC_TERMINATION_MESSAGE;
    int recv_message;
    int ready;

    MPI_Request send_request[comm_sz];
    MPI_Request recv_request[comm_sz];

    //! random picking improvement
    int nodes_of_group[node_group_total_nodes];
    int in = 0;
    int coordinator_of_current_group_rank = FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes);
    for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        nodes_of_group[in++] = rank;
    }

    //! random picking
    shuffle(nodes_of_group, node_group_total_nodes);

    /*for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        if (rank != my_rank)
        {
            COUNT_MPI_TIME_START
            MPI_Isend(&term_message, 1, MPI_INT, rank, WORKSTEALING_QUERY_ANSWERING_COMPLETION, MPI_COMM_WORLD, &send_request[rank]);
            COUNT_MPI_TIME_END
            if (ENABLE_PRINTS_WORKSTEALING)
            {
                printf("[QA-COMPLETION-SENDING - WORKSTEALING] - Node %d send (async) to node %d that he finished working.\n", my_rank, rank);
            }
        }
    }*/

    //! random picking
    for (int i = 0; i < node_group_total_nodes; i++)
    {
        int rank = nodes_of_group[i];
        if (rank != my_rank)
        {
            COUNT_MPI_TIME_START
            MPI_Isend(&term_message, 1, MPI_INT, rank, WORKSTEALING_QUERY_ANSWERING_COMPLETION, MPI_COMM_WORLD, &send_request[rank]);
            COUNT_MPI_TIME_END
            if (ENABLE_PRINTS_WORKSTEALING)
            {
                printf("[QA-COMPLETION-SENDING - WORKSTEALING] - Node %d send (async) to node %d that he finished working.\n", my_rank, rank);
            }
        }
    }

    /*for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        if (rank != my_rank)
        {
            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Irecv(&recv_message, 1, MPI_INT, rank, WORKSTEALING_QUERY_ANSWERING_COMPLETION, MPI_COMM_WORLD, &recv_request[rank]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END
        }
    }*/

    //! random picking
    for (int i = 0; i < node_group_total_nodes; i++)
    {
        int rank = nodes_of_group[i];
        if (rank != my_rank)
        {
            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Irecv(&recv_message, 1, MPI_INT, rank, WORKSTEALING_QUERY_ANSWERING_COMPLETION, MPI_COMM_WORLD, &recv_request[rank]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END
        }
    }

    /*int working_nodes[comm_sz];
    for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        working_nodes[rank] = 1;
    }*/

    //! random picking
    int working_nodes[node_group_total_nodes];
    for (int i = 0; i < node_group_total_nodes; i++)
    {
        working_nodes[i] = 1;
    }
    int finished_nodes = 1;

    while (finished_nodes < node_group_total_nodes)
    {
        //! random picking
        for (int i = 0; i < node_group_total_nodes; i++)
        {
            int rank = nodes_of_group[i];
            if (rank != my_rank && working_nodes[i] != 0)
            {
                COUNT_MPI_TIME_START
                MPI_Test(&recv_request[rank], &ready, MPI_STATUS_IGNORE);
                COUNT_MPI_TIME_END

                if (ready)
                {
                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[QA-COMPLETION-RECV - WORKSTEALING] - Node %d found that node %d has already finished his work.\n", my_rank, rank);
                    }
                    working_nodes[i] = 0;
                    finished_nodes++;
                }

                else
                {
                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[WORKSTEALING HELPER NODE] - Node %d found that node %d is still working.\n", my_rank, rank);
                    }

                    // send to node rank that you want to help
                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[WORKSTEALING HELPER NODE] - Node %d informed node %d that he wants to help\n", my_rank, rank);
                    }

                    COUNT_MPI_TIME_START
                    MPI_Isend(&term_message, 1, MPI_INT, rank, WORKSTEALING_INFORM_AVAILABILITY, MPI_COMM_WORLD, &send_request[rank]);
                    COUNT_MPI_TIME_END

                    float datas[2 + batches_to_send];

                    COUNT_COMMUNICATION_TIME_START
                    COUNT_MPI_TIME_START

                    // MPI_Recv(datas, 2 + batches_to_send, MPI_FLOAT, rank, WORKSTEALING_DATA_SEND, MPI_COMM_WORLD, MPI_STATUS_IGNORE); //! do irecv
                    MPI_Request data_req;
                    MPI_Irecv(datas, 2 + batches_to_send, MPI_FLOAT, rank, WORKSTEALING_DATA_SEND, MPI_COMM_WORLD, &data_req);

                    int req_ready;
                    MPI_Test(&data_req, &req_ready, MPI_STATUS_IGNORE);

                    while (!req_ready)
                    {
                        MPI_Test(&data_req, &req_ready, MPI_STATUS_IGNORE);

                        MPI_Test(&recv_request[rank], &ready, MPI_STATUS_IGNORE);
                        if (ready)
                        {
                            if (ENABLE_PRINTS_WORKSTEALING)
                            {
                                printf("[QA-COMPLETION-RECV - WORKSTEALING] - Node %d found that node %d has already finished his work.\n", my_rank, rank);
                            }
                            working_nodes[i] = 0;
                            finished_nodes++;

                            req_ready = 1;
                        }
                    }

                    COUNT_MPI_TIME_END
                    COUNT_COMMUNICATION_TIME_END
                    if (working_nodes[i] == 0)
                    {
                        break; //! is this okay?
                    }

                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[WORKSTEALING HELPER NODE] - Node %d received message from node %d :  [ %f,%f, ", my_rank, rank, datas[0], datas[1]);
                        for (int t = 0; t < batches_to_send; t++)
                        {
                            printf("%f ", datas[t + 2]);
                        }
                        printf("] .\n");
                    }

                    int query_num = datas[0];
                    float bsf = datas[1];

                    double prev_query_answering_time = 0;
                    // If query_num >= 0 the node has actual work to do.
                    if (query_num >= 0)
                    {
                        int batches_to_create[batches_to_send];
                        for (int i = 0; i < batches_to_send; i++)
                        {
                            batches_to_create[i] = datas[2 + i];
                        }

                        prev_query_answering_time = query_answering_time;
                        paa_from_ts(queries[query_num].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

                        ws_search_function_params ws_params;
                        ws_params.ts = queries[query_num].query;
                        ws_params.paa = paa;
                        ws_params.query_id = query_num;
                        ws_params.index = index;
                        ws_params.minimum_distance = minimum_distance;
                        ws_params.nodelist = &nodelist;
                        ws_params.bsf = bsf;
                        ws_params.estimation_func = estimation_func;
                        ws_params.batch_ids = batches_to_create;
                        ws_params.shared_bsf_results = shared_bsf_results;
                        ws_params.comm_data = NULL;
                        query_result result = ws_search_function(ws_params);

                        if (results[query_num] > result.distance)
                        {
                            results[query_num] = result.distance;
                        }

                        COUNT_QUERY_ANSWERING_TIME_END
                        COUNT_OUTPUT_TIME_END
                        double time_for_the_query = query_answering_time - prev_query_answering_time;
                        save_query_stats_chatzakis(query_num, result, time_for_the_query, queries_times);
                        printf("[MC] - Node: %d, query: %d, result: %f, time: %f, pqs: %d, pqs_stolen: %d\n", my_rank, query_num, results[query_num], time_for_the_query / 1000000, result.total_pqs, result.stolen_pqs);
                        COUNT_OUTPUT_TIME_START
                        COUNT_QUERY_ANSWERING_TIME_START
                    }
                }
            }
        }

        /*
        for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
        {
            if (rank != my_rank && working_nodes[rank] != 0)
            {
                COUNT_MPI_TIME_START
                MPI_Test(&recv_request[rank], &ready, MPI_STATUS_IGNORE);
                COUNT_MPI_TIME_END

                if (ready)
                {
                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[QA-COMPLETION-RECV - WORKSTEALING] - Node %d found that node %d has already finished his work.\n", my_rank, rank);
                    }
                    working_nodes[rank] = 0;
                    finished_nodes++;
                }
                else
                {
                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[WORKSTEALING HELPER NODE] - Node %d found that node %d is still working.\n", my_rank, rank);
                    }

                    // working_nodes[rank] = 1;
                    // send to node rank that you want to help
                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[WORKSTEALING HELPER NODE] - Node %d informed node %d that he wants to help\n", my_rank, rank);
                    }
                    COUNT_MPI_TIME_START
                    MPI_Isend(&term_message, 1, MPI_INT, rank, WORKSTEALING_INFORM_AVAILABILITY, MPI_COMM_WORLD, &send_request[rank]);
                    COUNT_MPI_TIME_END

                    float datas[2 + batches_to_send];

                    COUNT_COMMUNICATION_TIME_START
                    COUNT_MPI_TIME_START

                    // MPI_Recv(datas, 2 + batches_to_send, MPI_FLOAT, rank, WORKSTEALING_DATA_SEND, MPI_COMM_WORLD, MPI_STATUS_IGNORE); //! do irecv
                    MPI_Request data_req;
                    MPI_Irecv(datas, 2 + batches_to_send, MPI_FLOAT, rank, WORKSTEALING_DATA_SEND, MPI_COMM_WORLD, &data_req);

                    int req_ready;
                    MPI_Test(&data_req, &req_ready, MPI_STATUS_IGNORE);

                    while (!req_ready)
                    {
                        MPI_Test(&data_req, &req_ready, MPI_STATUS_IGNORE);

                        MPI_Test(&recv_request[rank], &ready, MPI_STATUS_IGNORE);
                        if (ready)
                        {
                            if (ENABLE_PRINTS_WORKSTEALING)
                            {
                                printf("[QA-COMPLETION-RECV - WORKSTEALING] - Node %d found that node %d has already finished his work.\n", my_rank, rank);
                            }
                            working_nodes[rank] = 0;
                            finished_nodes++;

                            req_ready = 1;
                        }
                    }

                    COUNT_MPI_TIME_END
                    COUNT_COMMUNICATION_TIME_END
                    if (working_nodes[rank] == 0)
                    {
                        break; //! is this okay?
                    }

                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[WORKSTEALING HELPER NODE] - Node %d received message from node %d :  [ %f,%f, ", my_rank, rank, datas[0], datas[1]);
                        for (int t = 0; t < batches_to_send; t++)
                        {
                            printf("%f ", datas[t + 2]);
                        }
                        printf("] .\n");
                    }

                    int query_num = datas[0];
                    float bsf = datas[1];

                    double prev_query_answering_time = 0;
                    // If query_num >= 0 the node has actual work to do.
                    if (query_num >= 0)
                    {
                        int batches_to_create[batches_to_send];
                        for (int i = 0; i < batches_to_send; i++)
                        {
                            batches_to_create[i] = datas[2 + i];
                        }

                        prev_query_answering_time = query_answering_time;
                        paa_from_ts(queries[query_num].query, paa, index->settings->paa_segments, index->settings->ts_values_per_paa_segment);

                        ws_search_function_params ws_params;
                        ws_params.ts = queries[query_num].query;
                        ws_params.paa = paa;
                        ws_params.query_id = query_num;
                        ws_params.index = index;
                        ws_params.minimum_distance = minimum_distance;
                        ws_params.nodelist = &nodelist;
                        ws_params.bsf = bsf;
                        ws_params.estimation_func = estimation_func;
                        ws_params.batch_ids = batches_to_create;
                        ws_params.shared_bsf_results = shared_bsf_results;
                        ws_params.comm_data = NULL;
                        query_result result = ws_search_function(ws_params);

                        results[query_num] = result.distance;

                        COUNT_QUERY_ANSWERING_TIME_END
                        COUNT_OUTPUT_TIME_END
                        double time_for_the_query = query_answering_time - prev_query_answering_time;
                        save_query_stats_chatzakis(query_num, result, time_for_the_query, queries_times);
                        printf("[MC] - Node: %d, query: %d, result: %f, time: %f, pqs: %d, pqs_stolen: %d\n", my_rank, query_num, results[query_num], time_for_the_query / 1000000, result.total_pqs, result.stolen_pqs);
                        COUNT_OUTPUT_TIME_START
                        COUNT_QUERY_ANSWERING_TIME_START
                    }
                    else
                    {
                        // working_nodes[rank] = 0;
                        // finished_nodes++;
                    }
                }
            }
        }
        */
    }

    free(paa);
}

/* UTILITIES */
double sigmoid_parameterized_chatzakis(double x)
{
    return SIG_m + (SIG_M - SIG_m) / (1 + SIG_b * exp(-SIG_c * (x - SIG_d)));
}

double (*initialize_basis_function(char *filename))(double)
{
    char basis_name[1024];
    FILE *f = fopen(filename, "r");
    fscanf(f, "%s", basis_name);
    // printf("Selected function: %s\n", basis_name);
    if (strcmp(basis_name, "sigmoid") == 0)
    {
        // m, M, b, c, d
        fscanf(f, "%lf %lf %lf %lf %lf", &SIG_m, &SIG_M, &SIG_b, &SIG_c, &SIG_d);
        // fscanf(f, "%f ", &SIG_M);
        // fscanf(f, "%f ", &SIG_b);
        // fscanf(f, "%f ", &SIG_c);
        // fscanf(f, "%f ", &SIG_d);
        // printf("Sigmoid Parameters: %f %f %f %f %f\n", SIG_m, SIG_M, SIG_b, SIG_c, SIG_d);
        return sigmoid_parameterized_chatzakis;
    }

    return NULL;
}

long int split_series_for_processes_chatzakis(long int dataset_size)
{
    long int dataset_size_per_process = dataset_size / comm_sz;
    long double times_series_per_process = ((long double)dataset_size) / comm_sz;

    if (IS_EVEN(comm_sz))
    {
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
        if (IS_LAST_NODE(my_rank, comm_sz))
        { // the last node must take one time series extra
            dataset_size_per_process++;
        }

        printf("[MC] - Process with rank [%d]: I will process %ld num of times series!\n", my_rank, dataset_size_per_process);
    }

    return dataset_size_per_process;
}

int generate_random_number_chatzakis(int max_number, int minimum_number)
{
    return rand() % (max_number + 1 - minimum_number) + minimum_number;
}

void save_query_stats_chatzakis(int q_loaded, query_result result, double time, double *queries_times)
{
    queries_times[q_loaded] = time;
    queries_statistics_manol[q_loaded].total_time = time;
    queries_statistics_manol[q_loaded].final_bsf = result.distance;
    queries_statistics_manol[q_loaded].lower_bound_distances = total_min_distance_counter;
    queries_statistics_manol[q_loaded].real_distances = total_real_distance_counter;

    for_all_queries_real_dist_counter += total_real_distance_counter;
    for_all_queries_min_dist_counter += total_min_distance_counter;
    total_real_distance_counter = 0;
    total_min_distance_counter = 0;

    sum_of_per_query_times += time;
}

FILE *fopen_validate_ds_file(const char *ifilename, int q_num, int ts_byte_size)
{
    if (comm_sz == 1)
        fprintf(stderr, ">>> Performing queries in file: %s\n", ifilename);

    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);
    file_position_type total_records = sz / ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);
    if (total_records < q_num)
    {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    return ifile;
}

void save_subtree_stats(int q_num, int amount)
{
    subtree_stats_file = fopen(subtree_stats_filename, "w+");

    if (!subtree_stats_file)
    {
        perror("Could not open the file to write the statistics for the subtrees");
        exit(EXIT_FAILURE);
    }

    fprintf(subtree_stats_file, "#query #subtree #height #total_nodes #dist(q,root) #min_dists #unpruned_nodes(leafs) #tree_BSF\n");
    fprintf(subtree_stats_file, "%d %d\n", q_num, amount);

    for (int i = 0; i < q_num; i++)
    {
        for (int j = 0; j < amount; j++)
        {
            fprintf(subtree_stats_file, "%d %d %d %d %f %d %d %f\n", i, j, subtree_stats[i][j].height, subtree_stats[i][j].total_nodes, subtree_stats[i][j].distance_from_root, subtree_stats[i][j].traversal_min_distance_calculations, subtree_stats[i][j].total_unpruned_nodes, subtree_stats[i][j].BSF_of_tree);
        }
    }

    fclose(subtree_stats_file);

    printf("Subtree results saved...\n");
}

void free_subtree_array(int q_num)
{
    for (int i = 0; i < q_num; i++)
    {
        free(subtree_stats[i]);
        subtree_stats[i] = NULL;
    }
    free(subtree_stats);
    subtree_stats = NULL;
}

void shuffle(int *array, size_t n)
{
    if (n > 1)
    {
        size_t i;
        for (i = 0; i < n - 1; i++)
        {
            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            int t = array[j];
            array[j] = array[i];
            array[i] = t;
        }
    }
}
