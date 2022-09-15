
#define _GNU_SOURCE

#ifdef VALUES
#include <values.h>
#endif
#include <float.h>
#include <errno.h>
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/wait.h>
#include <sched.h>

#include "omp.h"
#include "ads/isax_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"
#include "ads/inmemory_topk_engine.h"

#include <mpi.h>
#include <unistd.h>

#define left(i) ((i) << 1)
#define right(i) (((i) << 1) + 1)
#define parent(i) ((i) >> 1)

int NUM_PRIORITY_QUEUES;

inline void threadPin(int pid, int max_threads)
{
    int cpu_id;

    cpu_id = pid % max_threads;
    pthread_setconcurrency(max_threads);

    cpu_set_t mask;
    unsigned int len = sizeof(mask);

    CPU_ZERO(&mask);

    CPU_SET(cpu_id % max_threads, &mask); // OLD PINNING 1

    // if (cpu_id % 2 == 0)                                             // OLD PINNING 2
    //    CPU_SET(cpu_id % max_threads, &mask);
    // else
    //    CPU_SET((cpu_id + max_threads/2)% max_threads, &mask);

    // if (cpu_id % 2 == 0)                                             // FULL HT
    //    CPU_SET(cpu_id/2, &mask);
    // else
    //    CPU_SET((cpu_id/2) + (max_threads/2), &mask);

    // CPU_SET((cpu_id%4)*10 + (cpu_id%40)/4 + (cpu_id/40)*40, &mask);     // SOCKETS PINNING - Vader

    int ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");
}

// -------------------------------------
// -------------------------------------

// Botao's version
query_result exact_search_ParISnew_inmemory_hybrid(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                                                   float minimum_distance, int min_checked_leaves)
{
    //   RDcalculationnumber=0;
    // LBDcalculationnumber=0;
    query_result approximate_result = approximate_search_inmemory_pRecBuf(ts, paa, index);
    // query_result approximate_result = approximate_search_inmemory(ts, paa, index);
    query_result bsf_result = approximate_result;
    int tight_bound = index->settings->tight_bound;
    int aggressive_check = index->settings->aggressive_check;
    int node_counter = 0;
    // Early termination...
    if (approximate_result.distance == 0)
    {
        return approximate_result;
    }

    // EKOSMAS: REMOVED, 01 SEPTEMBER 2020
    // if(approximate_result.distance == FLT_MAX || min_checked_leaves > 1) {
    //     approximate_result = refine_answer_inmemory_m(ts, paa, index, approximate_result, minimum_distance, min_checked_leaves);
    // }

    if (maxquerythread == 1)
    {
        NUM_PRIORITY_QUEUES = 1;
    }
    else
    {
        NUM_PRIORITY_QUEUES = maxquerythread / 2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t *) * NUM_PRIORITY_QUEUES);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    int queuelabel[NUM_PRIORITY_QUEUES];

    query_result *do_not_remove = &approximate_result;

    SET_APPROXIMATE(approximate_result.distance);

    if (approximate_result.node != NULL)
    {
        // Insert approximate result in heap.
        // pqueue_insert(pq, &approximate_result);
        // GOOD: if(approximate_result.node->filename != NULL)
        // GOOD: printf("POPS: %.5lf\t", approximate_result.distance);
    }
    // Insert all root nodes in heap.
    isax_node *current_root_node = index->first_node;

    pthread_t threadid[maxquerythread];
    MESSI_workerdata workerdata[maxquerythread];
    pthread_mutex_t lock_queue = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t lock_current_root_node = PTHREAD_MUTEX_INITIALIZER;
    pthread_rwlock_t lock_bsf = PTHREAD_RWLOCK_INITIALIZER;
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size / NUM_PRIORITY_QUEUES,
                               cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i] = 1;
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].paa = paa;
        workerdata[i].ts = ts;
        workerdata[i].lock_queue = &lock_queue;
        workerdata[i].lock_current_root_node = &lock_current_root_node;
        workerdata[i].lock_bsf = &lock_bsf;
        workerdata[i].nodelist = nodelist->nlist;
        workerdata[i].amountnode = nodelist->node_amount;
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;
        workerdata[i].node_counter = &node_counter;
        workerdata[i].pq = allpq[i];
        workerdata[i].bsf_result = &bsf_result;
        workerdata[i].lock_barrier = &lock_barrier;
        workerdata[i].alllock = ququelock;
        workerdata[i].allqueuelabel = queuelabel;
        workerdata[i].allpq = allpq;
        workerdata[i].startqueuenumber = i % NUM_PRIORITY_QUEUES;
        workerdata[i].workernumber = i; // EKOSMAS, AUGUST 29 2020: Added
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue, (void *)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    // Free the nodes that where not popped.
    // Free the priority queue.
    pthread_barrier_destroy(&lock_barrier);

    // pqueue_free(pq);
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);
    bsf_result = bsf_result;

    // free(rfdata);
    //        printf("the number of LB distance calculation is %ld\t\t and the Real distance calculation is %ld\n ",LBDcalculationnumber,RDcalculationnumber);
    return bsf_result;

    // Free the nodes that where not popped.
}
// Botao's version
void *exact_search_worker_inmemory_hybridpqueue(void *rfdata)
{
    threadPin(((MESSI_workerdata *)rfdata)->workernumber, maxquerythread); // EKOSMAS, AUGUST 29 2020: Added

    isax_node *current_root_node;
    query_result *n;
    isax_index *index = ((MESSI_workerdata *)rfdata)->index;
    ts_type *paa = ((MESSI_workerdata *)rfdata)->paa;
    ts_type *ts = ((MESSI_workerdata *)rfdata)->ts;
    pqueue_t *pq = ((MESSI_workerdata *)rfdata)->pq;
    query_result *do_not_remove = ((MESSI_workerdata *)rfdata)->bsf_result;
    float minimum_distance = ((MESSI_workerdata *)rfdata)->minimum_distance;
    int limit = ((MESSI_workerdata *)rfdata)->limit;
    int checks = 0;
    bool finished = true;
    int current_root_node_number;
    int tight_bound = index->settings->tight_bound;
    int aggressive_check = index->settings->aggressive_check;
    query_result *bsf_result = (((MESSI_workerdata *)rfdata)->bsf_result);
    float bsfdisntance = bsf_result->distance;
    int calculate_node = 0, calculate_node_quque = 0;
    int tnumber = rand() % NUM_PRIORITY_QUEUES;
    int startqueuenumber = ((MESSI_workerdata *)rfdata)->startqueuenumber;
    // COUNT_QUEUE_TIME_START

    if (((MESSI_workerdata *)rfdata)->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1)
    {
        current_root_node_number = __sync_fetch_and_add(((MESSI_workerdata *)rfdata)->node_counter, 1);
        // printf("the number is %d\n",current_root_node_number );
        if (current_root_node_number >= ((MESSI_workerdata *)rfdata)->amountnode)
            break;
        current_root_node = ((MESSI_workerdata *)rfdata)->nodelist[current_root_node_number];

        insert_tree_node_m_hybridpqueue(paa, current_root_node, index, bsfdisntance, ((MESSI_workerdata *)rfdata)->allpq, ((MESSI_workerdata *)rfdata)->alllock, &tnumber);
        // insert_tree_node_mW(paa,current_root_node,index,bsfdisntance,pq,((MESSI_workerdata*)rfdata)->lock_queue);
    }

    // COUNT_QUEUE_TIME_END
    // calculate_node_quque=pq->size;

    pthread_barrier_wait(((MESSI_workerdata *)rfdata)->lock_barrier);
    // printf("the size of quque is %d \n",pq->size);

    if (((MESSI_workerdata *)rfdata)->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    while (1)
    {
        pthread_mutex_lock(&(((MESSI_workerdata *)rfdata)->alllock[startqueuenumber]));
        n = pqueue_pop(((MESSI_workerdata *)rfdata)->allpq[startqueuenumber]);
        pthread_mutex_unlock(&(((MESSI_workerdata *)rfdata)->alllock[startqueuenumber]));
        if (n == NULL)
            break;
        // pthread_rwlock_rdlock(((MESSI_workerdata*)rfdata)->lock_bsf);
        bsfdisntance = bsf_result->distance;
        // pthread_rwlock_unlock(((MESSI_workerdata*)rfdata)->lock_bsf);
        //  The best node has a worse mindist, so search is finished!

        if (n->distance > bsfdisntance || n->distance > minimum_distance)
        {
            break;
        }
        else
        {
            // If it is a leaf, check its real distance.
            if (n->node->is_leaf)
            {

                checks++;

                float distance = calculate_node_distance2_inmemory(index, n->node, ts, paa, bsfdisntance);
                if (distance < bsfdisntance)
                {
                    pthread_rwlock_wrlock(((MESSI_workerdata *)rfdata)->lock_bsf);
                    if (distance < bsf_result->distance)
                    {
                        bsf_result->distance = distance;
                        bsf_result->node = n->node;
                    }
                    pthread_rwlock_unlock(((MESSI_workerdata *)rfdata)->lock_bsf);
                }
            }
        }
        free(n);
    }

    if ((((MESSI_workerdata *)rfdata)->allqueuelabel[startqueuenumber]) == 1)
    {
        (((MESSI_workerdata *)rfdata)->allqueuelabel[startqueuenumber]) = 0;
        pthread_mutex_lock(&(((MESSI_workerdata *)rfdata)->alllock[startqueuenumber]));
        while (n = pqueue_pop(((MESSI_workerdata *)rfdata)->allpq[startqueuenumber]))
        {
            free(n);
        }
        pthread_mutex_unlock(&(((MESSI_workerdata *)rfdata)->alllock[startqueuenumber]));
    }

    if (((MESSI_workerdata *)rfdata)->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    while (1)
    {
        int offset = rand() % NUM_PRIORITY_QUEUES;
        finished = true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if ((((MESSI_workerdata *)rfdata)->allqueuelabel[i]) == 1)
            {
                finished = false;
                while (1)
                {
                    pthread_mutex_lock(&(((MESSI_workerdata *)rfdata)->alllock[i]));
                    n = pqueue_pop(((MESSI_workerdata *)rfdata)->allpq[i]);
                    pthread_mutex_unlock(&(((MESSI_workerdata *)rfdata)->alllock[i]));
                    if (n == NULL)
                        break;
                    if (n->distance > bsfdisntance || n->distance > minimum_distance)
                    {
                        break;
                    }
                    else
                    {
                        // If it is a leaf, check its real distance.
                        if (n->node->is_leaf)
                        {
                            checks++;
                            float distance = calculate_node_distance2_inmemory(index, n->node, ts, paa, bsfdisntance);
                            if (distance < bsfdisntance)
                            {
                                pthread_rwlock_wrlock(((MESSI_workerdata *)rfdata)->lock_bsf);
                                if (distance < bsf_result->distance)
                                {
                                    bsf_result->distance = distance;
                                    bsf_result->node = n->node;
                                }
                                pthread_rwlock_unlock(((MESSI_workerdata *)rfdata)->lock_bsf);
                            }
                        }
                    }
                    // add
                    free(n);
                }
            }
        }
        if (finished)
        {
            break;
        }
    }

    if (((MESSI_workerdata *)rfdata)->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }

    // pthread_barrier_wait(((MESSI_workerdata*)rfdata)->lock_barrier);
    // while(n=pqueue_pop(pq))
    //{
    // free(n);
    // }
    // pqueue_free(pq);
    //

    // printf("create pq time is %f \n",worker_total_time );
    // printf("the check's node is\t %d\tthe local queue's node is\t%d\n",checks,calculate_node_quque);
}
// Botao's version
void insert_tree_node_m_hybridpqueue(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber)
{
    // COUNT_CAL_TIME_START
    //  ??? EKOSMAS: Why not using SIMD version of the following function? I wait a coorect version of it by Botao
    float distance = minidist_paa_to_isax(paa, node->isax_values,
                                          node->isax_cardinalities,
                                          index->settings->sax_bit_cardinality,
                                          index->settings->sax_alphabet_cardinality,
                                          index->settings->paa_segments,
                                          MINVAL, MAXVAL,
                                          index->settings->mindist_sqrt);
    // COUNT_CAL_TIME_END

    if (distance < bsf)
    {
        if (node->is_leaf)
        {
            query_result *mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber = (*tnumber + 1) % NUM_PRIORITY_QUEUES;
            added_tree_node++;
        }
        else
        {
            if (node->left_child->isax_cardinalities != NULL)
            {
                insert_tree_node_m_hybridpqueue(paa, node->left_child, index, bsf, pq, lock_queue, tnumber);
            }
            if (node->right_child->isax_cardinalities != NULL)
            {
                insert_tree_node_m_hybridpqueue(paa, node->right_child, index, bsf, pq, lock_queue, tnumber);
            }
        }
    }
}

// -------------------------------------
// -------------------------------------

// ekosmas version
// called for each query

int mpi_error = MPI_SUCCESS;
#define CHECK_FOR_SUCCESS__OR__ABORT(ret_value)  \
    if (ret_value != MPI_SUCCESS)                \
    {                                            \
        printf("[%d] MPI Error!\n", my_rank);    \
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE); \
    }

// parameters
extern int share_with_nodes_bsf;
extern int all_nodes_index_all_given_dataset;

// variables
extern int my_rank;
extern int comm_sz;

extern MPI_Comm *communicators;
extern MPI_Request *requests;

// need to be an array of size = |NODES| because for each node the received bsf is stored into the appropriate possition
extern struct bsf_msg *shared_bsfs;

// statistics

extern int *bcasts_per_query; // otan stelno ego ena broadcast tote auksano tin my_rank thesi kata 1. Otan lambano ena bcast tote ayksano tin thesi i kata 1

// For each query new threads is created
int query_counter = 0;

__thread unsigned long long int thread_real_distance_counter = 0; // per query
__thread unsigned long long int thread_min_distance_counter = 0;  // per query

unsigned long long int total_real_distance_counter = 0; // total distances from all the threads for a specific query
unsigned long long int total_min_distance_counter = 0;

unsigned long long int for_all_queries_real_dist_counter = 0;
unsigned long long int for_all_queries_min_dist_counter = 0;

pthread_mutex_t distances_counter_mutex = PTHREAD_MUTEX_INITIALIZER;

int bsf_change_counter = 0;               // total times of bsf changes
int bsf_share_counter = 0;                // total times that i bcast-send a bsf to others
int bsf_receives_counter = 0;             // total times that i bcast-receive a bsf from others
int bsf_receives_wrong_query_counter = 0; // total times that i receive a bcast and the sequence number is different than my current processed query

/*This is times statistics about query threads*/
extern struct timeval *threads_start_time;
extern struct timeval *threads_curr_time;
extern double threads_total_difference;

extern query_statistics_t *queries_statistics_manol;

// Chatzakis - Query Prediction Stats
extern char produce_query_prediction_statistics;
extern long int per_query_total_bsf_changes;
extern char *query_statistics_filename;
extern FILE *query_statistics_file;
extern struct timeval bsf_start_time, bsf_end_time;
extern double elapsed_time_for_bsf;
extern volatile long int per_query_leafs_visited;

pthread_mutex_t LEAFS_MUTEX_QUERY_PREDICTION_STATISTICS;

extern per_subtree_stats **subtree_stats;
extern char gather_subtree_stats;
extern FILE *subtree_stats_file;
extern char *subtree_stats_filename;

extern char *pq_stats_filename;
extern FILE *pq_stats_file;
extern char gather_pq_stats;

extern double SIG_M;
extern double SIG_m;
extern double SIG_b;
extern double SIG_c;
extern double SIG_d;

extern int th_div_factor;
extern int batches_to_send;

extern int workstealing_share_bsf;
extern int bsf_sharing_pdr;

extern long int node_groups_number;
extern pdr_node_group *node_groups;
extern long int node_group_total_nodes;
extern int bsf_sharing_pdr;

void update_share_bsf_statistics()
{
    // statistics
    if (shared_bsfs[my_rank].q_num == query_counter)
    { // another change here, i->my_rank
        bsf_receives_counter++;
    }
    else
    {
        bsf_receives_wrong_query_counter++;
    }
    bcasts_per_query[my_rank] += 1; // i have receive one bcast //another change here, i->my_rank
}

void update_threads_statistics()
{
    // statistics
    double max_thread_time = DBL_MIN;
    for (int i = 0; i < maxquerythread; i++)
    {

        // converts seconds to microseconds
        double tS = threads_start_time[i].tv_sec * 1000000 + (threads_start_time[i].tv_usec);
        double tE = threads_curr_time[i].tv_sec * 1000000 + (threads_curr_time[i].tv_usec);
        double elapsed_time = (tE - tS);

        if (elapsed_time > max_thread_time)
        {
            max_thread_time = elapsed_time;
        }
    }
    threads_total_difference += max_thread_time;
}

// bcast the initial bsf value (to its own communicator) as sender
// and bcast (as receiver) to all the other communicators
void share_initial_bsf(query_result bsf_result)
{ // CHANGE HERE: passed bsf_result as arg

    shared_bsfs[my_rank].q_num = query_counter;
    shared_bsfs[my_rank].bsf = bsf_result.distance;

    // for each node, i broadcast
    // i am sender only when i == my_rank
    // all the other times i am receiver
    for (int i = 0; i < comm_sz; ++i)
    {
        if (my_rank == i)
        {
            DP(printf("[%d] q-%d approximate: %f (Ibcast)\n", my_rank, query_counter, bsf_result.distance);)
            bcasts_per_query[my_rank] += 1; // i send a broadcast
            bsf_share_counter++;
        }
        mpi_error = MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]);
        CHECK_FOR_SUCCESS__OR__ABORT(mpi_error)
    }
}

// receives any initial bsf value from other nodes, if there is one
float try_to_receive_initial_bsfs() //=TODO block
{
    for (int i = 0; i < comm_sz; ++i)
    {
        int ready;
        MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);

        if (my_rank != i && ready)
        { // there is a mesage from node with id = i
            DP(printf("[@@@%d] q-%d I received bsf from [%d] with value: %f!\n", my_rank, query_counter, i, shared_bsfs[i].bsf);)

            // check if the received value is smaller that my value
            if (shared_bsfs[i].bsf < shared_bsfs[my_rank].bsf && shared_bsfs[i].q_num == query_counter)
            {
                // if so update to the smallest value
                shared_bsfs[my_rank].bsf = shared_bsfs[i].bsf;
                shared_bsfs[my_rank].q_num = query_counter;
            }
            update_share_bsf_statistics();

            // i make again a broadcast, as receiver! (because my_rank != i)
            MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]);
        }
    }

    return shared_bsfs[my_rank].bsf;
}

float receive_initialBSF_blocking_chatzakis()
{

    printf("=================== [MC] -- Custom blocking bsf -- Node %d\n", my_rank);

    for (int i = 0; i < comm_sz; ++i)
    {
        int ready;
        MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);

        if (my_rank != i)
        { // dont reproc your own bsf

            printf("[MC] -- Node %d waiting to recieve a BSF from node %d.\n", my_rank, i);
            while (!ready)
            {
                MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);
            }

            DP(printf("[@@@%d] q-%d I received bsf from [%d] with value: %f!\n", my_rank, query_counter, i, shared_bsfs[i].bsf);)

            // check if the received value is smaller that my value
            if (shared_bsfs[i].bsf < shared_bsfs[my_rank].bsf && shared_bsfs[i].q_num == query_counter)
            {
                // if so update to the smallest value
                shared_bsfs[my_rank].bsf = shared_bsfs[i].bsf;
                shared_bsfs[my_rank].q_num = query_counter;
            }
            update_share_bsf_statistics();

            // i make again a broadcast, as receiver! (because my_rank != i)
            MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]);
        }
    }

    return shared_bsfs[my_rank].bsf;
}

// ! -------------- BSF-SHARING - CHATZAKIS --------------
void receive_shared_bsf_chatzakis(/*MESSI_workerdata_ekosmas *input_data*/ int workernumber, query_result *bsf_result, float *shared_bsf_results, pthread_mutex_t *lock_bsf)
{

    if (/*input_data->*/ workernumber != 0 || !bsf_sharing_pdr)
    {
        return;
    }

    // query_result *bsf_result = input_data->bsf_result;

    for (int i = 0; i < comm_sz; ++i)
    {
        int ready;
        MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);

        if (my_rank != i && ready)
        { // i received from other node a bsf value
            do
            {
                float bsf_received = shared_bsfs[i].bsf;
                float current_bsf = shared_bsfs[my_rank].bsf;

                int query_id = shared_bsfs[i].q_num;

                if (ENABLE_PRINTS_BSF_SHARING /*&& (my_rank == 2 || my_rank == 3)*/)
                {
                    // printf("[BSF SHARING] - Node %d received a bsf-msg (%d, %f) from node %d. QueryCounter = %d\n", my_rank, query_id, bsf_received, i, query_counter);
                }

                if (bsf_received < /*input_data->*/ shared_bsf_results[query_id])
                {
                    if (ENABLE_PRINTS_BSF_SHARING /*&& (my_rank == 2 || my_rank == 3) && (i == 0 || i == 1)*/)
                    {
                        printf("[BSF SHARING] - Node %d received an improving bsf-msg (%d, %f) from node %d. QueryCounter = %d\n", my_rank, query_id, bsf_received, query_counter);
                    }

                    /*input_data->*/ shared_bsf_results[query_id] = bsf_received; //! Keeping the other results
                }

                // the received bsf is smaller from what i have
                if (shared_bsfs[i].bsf < shared_bsfs[my_rank].bsf && shared_bsfs[i].bsf < bsf_result->distance && shared_bsfs[i].q_num == query_counter)
                {
                    //! Update if the current
                    shared_bsfs[my_rank].bsf = shared_bsfs[i].bsf;
                    shared_bsfs[my_rank].q_num = query_counter;

                    /*input_data->*/ shared_bsf_results[query_id] = bsf_received; //! check again..
                }

                update_share_bsf_statistics();

                MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]); // as a listener, because at some point some other node can send my a new bsf value
                MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);
            } while (ready);

            // Change the bsf for the prunning
            // need lock, because other local thread may try to update the bsf value as well
            pthread_mutex_lock(/*input_data->*/ lock_bsf);
            bsf_result->distance = shared_bsfs[my_rank].bsf;
            pthread_mutex_unlock(/*input_data->*/ lock_bsf);
        }
    }
}

void share_bsf_chatzakis(/*MESSI_workerdata_ekosmas *input_data*/ query_result *bsf_result, int workernumber)
{
    // query_result *bsf_result = input_data->bsf_result;
    // int workernumber = input_data->workernumber;

    // check to get the bsf from other nodes.
    if (bsf_sharing_pdr && workernumber == 0) //! this should be done atomically, really careful with this one!
    {
        int ready = 1;
        float current_BSF = bsf_result->distance;

        int bsf_changed = (current_BSF != shared_bsfs[my_rank].bsf);

        MPI_Test(&requests[my_rank], &ready, MPI_STATUS_IGNORE);
        // MPI_Wait(&requests[my_rank], MPI_STATUS_IGNORE);

        if (ready && bsf_changed)
        {
            bcasts_per_query[my_rank] += 1;
            shared_bsfs[my_rank].bsf = bsf_result->distance;
            shared_bsfs[my_rank].q_num = query_counter;

            if (ENABLE_PRINTS_BSF_SHARING /*&& (my_rank == 0 || my_rank == 1)*/)
            {
                printf("[BSF-SENDER] - Node %d sends an improvement BSF = (%d, %f)\n", my_rank, shared_bsfs[my_rank].q_num, shared_bsfs[my_rank].bsf);
            }

            bsf_share_counter++;
            MPI_Ibcast(&shared_bsfs[my_rank], 1, bsf_msg_type, my_rank, communicators[my_rank], &requests[my_rank]);
        }
    }
}

void share_bsf_chatzakis_parent_func(query_result *bsf_result)
{
    int ready = 1;
    float current_BSF = bsf_result->distance;

    MPI_Test(&requests[my_rank], &ready, MPI_STATUS_IGNORE);
    // MPI_Wait(&requests[my_rank], MPI_STATUS_IGNORE);

    if (ready)
    {
        bcasts_per_query[my_rank] += 1;
        shared_bsfs[my_rank].bsf = bsf_result->distance;
        shared_bsfs[my_rank].q_num = query_counter;

        if (ENABLE_PRINTS_BSF_SHARING && (my_rank == 0 || my_rank == 1))
        {
            printf("[BSF-SENDER] - Node %d sends an improvement BSF = (%d, %f)\n", my_rank, shared_bsfs[my_rank].q_num, shared_bsfs[my_rank].bsf);
        }

        bsf_share_counter++;
        MPI_Ibcast(&shared_bsfs[my_rank], 1, bsf_msg_type, my_rank, communicators[my_rank], &requests[my_rank]);
    }
}

void share_initial_bsf_chatzakis(query_result bsf_result) //! sus
{                                                         // CHANGE HERE: passed bsf_result as arg
    shared_bsfs[my_rank].q_num = query_counter;
    shared_bsfs[my_rank].bsf = bsf_result.distance;

    // for each node, i broadcast
    // i am sender only when i == my_rank
    // all the other times i am receiver
    for (int i = 0; i < comm_sz; ++i)
    {
        if (my_rank == i)
        {
            DP(printf("[%d] q-%d approximate: %f (Ibcast)\n", my_rank, query_counter, bsf_result.distance);)
            bcasts_per_query[my_rank] += 1; // i send a broadcast
            bsf_share_counter++;

            if (ENABLE_PRINTS_BSF_SHARING && (my_rank == 0 || my_rank == 1))
            {
                printf("[BSF-SENDER-INITIAL] - Node %d sends an improvement BSF = (%d, %f)\n", my_rank, shared_bsfs[my_rank].q_num, shared_bsfs[my_rank].bsf);
            }
        }
        mpi_error = MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]);
        CHECK_FOR_SUCCESS__OR__ABORT(mpi_error)
    }
}

float try_to_receive_initial_bsfs_chatzakis(float *shared_results)
{

    for (int i = 0; i < comm_sz; ++i)
    {
        int ready;
        MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);

        if (my_rank != i && ready)
        { // there is a mesage from node with id = i
            // DP(printf("[@@@%d] q-%d I received bsf from [%d] with value: %f!\n", my_rank, query_counter, i, shared_bsfs[i].bsf);)

            float bsf_received = shared_bsfs[i].bsf;
            float current_bsf = shared_bsfs[my_rank].bsf;

            int query_id = shared_bsfs[i].q_num;

            if (ENABLE_PRINTS_BSF_SHARING && (my_rank == 2 || my_rank == 3))
            {
                printf("[BSF SHARING] - Node %d received a bsf-msg (%d, %f) from node %d. QueryCounter = %d\n", my_rank, query_id, bsf_received, i, query_counter);
            }

            if (bsf_received < shared_results[query_id])
            {
                if (ENABLE_PRINTS_BSF_SHARING && (my_rank == 2 || my_rank == 3))
                {
                    printf("[BSF SHARING] - Node %d received an improving bsf-msg (%d, %f) from node %d. QueryCounter = %d\n", my_rank, query_id, bsf_received, query_counter);
                }
                shared_results[query_id] = bsf_received;
            }

            // check if the received value is smaller that my value
            if (shared_bsfs[i].bsf < shared_bsfs[my_rank].bsf && shared_bsfs[i].q_num == query_counter)
            {
                // if so update to the smallest value
                shared_bsfs[my_rank].bsf = shared_bsfs[i].bsf;
                shared_bsfs[my_rank].q_num = query_counter;
            }

            update_share_bsf_statistics();

            // i make again a broadcast, as receiver! (because my_rank != i)
            MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]);
        }
    }

    return shared_bsfs[my_rank].bsf;
}

extern int gather_init_bsf_only;
/**
 * @brief MESSI exact search
 * 
 * @param args Exact search arguments 
 * @return query_result Contains the float result of the query
 */
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas(search_function_params args)
{
    static int first_time_flag = 1;

    ts_type *ts = args.ts;
    ts_type *paa = args.paa;

    isax_index *index = args.index;
    node_list *nodelist = args.nodelist;

    float minimum_distance = args.minimum_distance;
    float *shared_bsf_results = args.shared_bsf_results;

    query_counter = args.query_id;

    // printf(" == [MC] -- Started similarity search. Obtaining approximate result.\n");
    if (produce_query_prediction_statistics)
    {
        per_query_leafs_visited = 0;
        per_query_total_bsf_changes = 0;

        gettimeofday(&bsf_start_time, NULL);
    }

    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);
    query_result init_bsf = bsf_result;

    if (gather_init_bsf_only)
    {
        return bsf_result;
    }

    /*if (produce_query_prediction_statistics)
    {
        gettimeofday(&bsf_end_time, NULL);

        per_query_total_bsf_changes++;
        elapsed_time_for_bsf = (bsf_end_time.tv_sec - bsf_start_time.tv_sec) * 1000.0;    // sec to ms
        elapsed_time_for_bsf += (bsf_end_time.tv_usec - bsf_start_time.tv_usec) / 1000.0; // us to ms
        per_query_leafs_visited++;

        fprintf(query_statistics_file, "%d %d %f %d %f\n", query_counter + 1, per_query_total_bsf_changes, bsf_result.distance, per_query_leafs_visited, elapsed_time_for_bsf);
    }*/

    // printf(" == [MC] -- After approximate search bsf: [%f] [%f]\n", bsf_result.distance, bsf_result.init_bsf);

    SET_APPROXIMATE(bsf_result.distance);

    queries_statistics_manol[query_counter].initial_bsf = bsf_result.distance;

    if (share_with_nodes_bsf)
    {
        share_initial_bsf(bsf_result);
    }

    if (bsf_sharing_pdr && first_time_flag)
    {
        share_initial_bsf_chatzakis(bsf_result);
    }

    if (bsf_result.distance == 0)
    {
        return bsf_result;
    }

    if (maxquerythread == 1)
    {
        NUM_PRIORITY_QUEUES = 1;
    }
    else
    {
        NUM_PRIORITY_QUEUES = maxquerythread / 2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t *) * NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES]; // EKOSMAS, 21 SEPTEMBER 2020: REMOVED

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER; // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size / NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i] = 1;
    }

    volatile int node_counter = 0; // EKOSMAS, 29 AUGUST 2020: added volatile
    // 26-02-2021
    // node ammount / 2 set with appropriate value
    // procces_node end and pass it into input data

    // how many subtrees must a node process
    if (all_nodes_index_all_given_dataset == 1)
    {
        node_counter = my_rank * (nodelist->node_amount / comm_sz);
    }

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                            // query ts
        workerdata[i].paa = paa;                          // query paa
        workerdata[i].lock_bsf = &lock_bsf;               // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;         // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount; // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;        // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;               // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                   // the BSF
        workerdata[i].lock_barrier = &lock_barrier;               // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                        // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                 // ??? How is this used?
        workerdata[i].allpq = allpq;                              // priority queues
        workerdata[i].startqueuenumber = i % NUM_PRIORITY_QUEUES; // initial priority queue to start
        workerdata[i].workernumber = i;

        workerdata[i].workstealing_round = -1;
        workerdata[i].workstealing_on_which_tree_i_work = WORKSTEALING_WORK_ON_MY_TREE;

        workerdata[i].shared_bsf_results = shared_bsf_results;
    }

    // check if there is arrive a bsf message from the other nodes and change the bsf related to prunning
    if (share_with_nodes_bsf)
    {
        bsf_result.distance = try_to_receive_initial_bsfs(); // [MC] change: Made this func blocking
        // bsf_result.distance = receive_initialBSF_blocking_chatzakis();
    }

    if (bsf_sharing_pdr)
    {
        float candidate_bsf = try_to_receive_initial_bsfs_chatzakis(shared_bsf_results);

        if (shared_bsf_results[query_counter] < candidate_bsf)
        {
            candidate_bsf = shared_bsf_results[query_counter];
        }

        bsf_result.distance = candidate_bsf;
    }

    // threads creations
    // printf("[MC] ----------------------------------------- Search Threads: %d\n", maxquerythread);

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas, (void *)&(workerdata[i]));
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    update_threads_statistics();
    pthread_barrier_destroy(&lock_barrier);

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    if (produce_query_prediction_statistics)
    {
        gettimeofday(&bsf_end_time, NULL);

        elapsed_time_for_bsf = (bsf_end_time.tv_sec - bsf_start_time.tv_sec) * 1000.0;    // sec to ms
        elapsed_time_for_bsf += (bsf_end_time.tv_usec - bsf_start_time.tv_usec) / 1000.0; // us to ms

        fprintf(query_statistics_file, "%d %d %f %d %f\n", query_counter + 1, 0, init_bsf.distance, 0, elapsed_time_for_bsf);
    }

    return bsf_result;
}

// if i have found a new bsf value, i must bcast it to the other nodes
// and check if there a new bsf message from the other node as well
// called while processing the priority queues
void share_bsf(MESSI_workerdata_ekosmas *input_data)
{
    query_result *bsf_result = input_data->bsf_result;
    int workernumber = input_data->workernumber;

    // check to get the bsf from other nodes.
    if (share_with_nodes_bsf)
    {
        if (workernumber == 0)
        { // only the thread with id 0 run this code (coordinator thread)

            float my_prev_bsf = shared_bsfs[my_rank].bsf; // my_old_bsf
            int found_new_bsf = ((my_prev_bsf != bsf_result->distance)) ? true : false;

            for (int i = 0; i < comm_sz; ++i)
            {
                int ready;
                MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);

                if (my_rank == i && ready && found_new_bsf)
                {
                    // SEND MY NEW BSF TO OTHER MPI-NODES
                    bcasts_per_query[i] += 1; // my_rank
                    shared_bsfs[i].bsf = bsf_result->distance;
                    shared_bsfs[i].q_num = query_counter;

                    bsf_share_counter++;
                    MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]);
                }
                else if (my_rank != i && ready)
                { // i received from other node a bsf value
                    do
                    {
                        // the received bsf is smaller from what i have
                        if (shared_bsfs[i].bsf < shared_bsfs[my_rank].bsf && shared_bsfs[i].bsf < bsf_result->distance && shared_bsfs[i].q_num == query_counter)
                        {
                            shared_bsfs[my_rank].bsf = shared_bsfs[i].bsf;
                            shared_bsfs[my_rank].q_num = query_counter;
                        }

                        update_share_bsf_statistics();

                        MPI_Ibcast(&shared_bsfs[i], 1, bsf_msg_type, i, communicators[i], &requests[i]); // as a listener, because at some point some other node can send my a new bsf value

                        MPI_Test(&requests[i], &ready, MPI_STATUS_IGNORE);
                    } while (ready);

                    // Change the bsf for the prunning
                    // need lock, because other local thread may try to update the bsf value as well
                    pthread_mutex_lock(input_data->lock_bsf);
                    bsf_result->distance = shared_bsfs[my_rank].bsf;
                    pthread_mutex_unlock(input_data->lock_bsf);
                }
            }
        }
    }
}

extern int simple_work_stealing;
int process_queue_node_ekosmas(MESSI_workerdata_ekosmas *input_data, int i, int *checks, int workernumber)
{
    // pthread_mutex_lock(&(input_data->alllock[i])); // old code
    while (pthread_mutex_trylock(&(input_data->alllock[i])) == EBUSY)
    {                          // new code
        share_bsf(input_data); // is thread == zero it will check for messages

        share_bsf_chatzakis(input_data->bsf_result, input_data->workernumber);
        receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
    }

    query_result *n = pqueue_pop(input_data->allpq[i]);
    pthread_mutex_unlock(&(input_data->alllock[i]));
    if (n == NULL)
    {
        return 0;
    }

    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    if (n->distance > bsfdisntance || n->distance > input_data->minimum_distance)
    { // The best node has a worse mindist, so search is finished!
        return 0;
    }
    else
    {
        // If it is a leaf, check its real distance.
        if (n->node->is_leaf)
        {
            if (produce_query_prediction_statistics)
            {
                __sync_fetch_and_add(&per_query_leafs_visited, 1);
            }

            (*checks)++; // manol ??? counter for computed real distances????               // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas(input_data->index, n->node, input_data->ts, input_data->paa, bsfdisntance, input_data, bsf_result);
            if (distance < bsf_result->distance)
            {

                // pthread_mutex_lock(input_data->lock_bsf); // old code
                while (pthread_mutex_trylock(input_data->lock_bsf) == EBUSY)
                {
                    share_bsf(input_data); // is thread == zero it will check for messages

                    share_bsf_chatzakis(bsf_result, input_data->workernumber);
                    receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf); //! check for error..
                }

                if (distance < bsf_result->distance)
                {
                    // statistics
                    bsf_change_counter++;

                    bsf_result->distance = distance;
                    bsf_result->node = n->node;

                    share_bsf_chatzakis(bsf_result, input_data->workernumber);

                    // printf("Node %d improved the bsf to %f for query %d\n", my_rank, distance, query_counter);

                    if (produce_query_prediction_statistics)
                    {
                        /*gettimeofday(&bsf_end_time, NULL);

                        per_query_total_bsf_changes++;
                        elapsed_time_for_bsf = (bsf_end_time.tv_sec - bsf_start_time.tv_sec) * 1000.0;    // sec to ms
                        elapsed_time_for_bsf += (bsf_end_time.tv_usec - bsf_start_time.tv_usec) / 1000.0; // us to ms

                        //fprintf(query_statistics_file, "%d %d %f %d %f\n", query_counter + 1, per_query_total_bsf_changes, bsf_result->distance, per_query_leafs_visited, elapsed_time_for_bsf);
                        */
                    }
                }
                pthread_mutex_unlock(input_data->lock_bsf);
            }
        }
    }
    free(n);

    return 1;
}
// ekosmas version

int process_queue_node_chatzakis(MESSI_workerdata_ekosmas *input_data, int i, int *checks, int workernumber, communication_module_data *comm_data)
{
    // pthread_mutex_lock(&(input_data->alllock[i])); // old code
    while (pthread_mutex_trylock(&(input_data->alllock[i])) == EBUSY)
    {                          // new code
        share_bsf(input_data); // is thread == zero it will check for messages

        share_bsf_chatzakis(input_data->bsf_result, input_data->workernumber);
        receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
        if (comm_data != NULL && workernumber == 0 && comm_data->mode == MODULE)
        {
            CALL_MODULE
        }
    }

    query_result *n = pqueue_pop(input_data->allpq[i]);
    pthread_mutex_unlock(&(input_data->alllock[i]));
    if (n == NULL)
    {
        return 0;
    }

    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    if (n->distance > bsfdisntance || n->distance > input_data->minimum_distance)
    { // The best node has a worse mindist, so search is finished!
        return 0;
    }
    else
    {
        // If it is a leaf, check its real distance.
        if (n->node->is_leaf)
        {
            (*checks)++; // manol ??? counter for computed real distances????               // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas(input_data->index, n->node, input_data->ts, input_data->paa, bsfdisntance, input_data, bsf_result);
            if (distance < bsf_result->distance)
            {
                while (pthread_mutex_trylock(input_data->lock_bsf) == EBUSY)
                {
                    share_bsf(input_data); // is thread == zero it will check for messages

                    share_bsf_chatzakis(bsf_result, input_data->workernumber);
                    receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
                }

                if (distance < bsf_result->distance)
                {
                    bsf_change_counter++;

                    bsf_result->distance = distance;
                    bsf_result->node = n->node;

                    share_bsf_chatzakis(bsf_result, input_data->workernumber);

                    // printf("Node %d improved the bsf to %f for query %d\n", my_rank, distance, query_counter);

                    if (comm_data != NULL && workernumber == 0 && comm_data->mode == MODULE) // it is an atomic operation
                    {
                        CALL_MODULE
                    }
                }

                pthread_mutex_unlock(input_data->lock_bsf);
            }
        }
    }
    free(n);

    return 1;
}
// ekosmas version
// tmp put here.
long int find_total_nodes_tmp(isax_node *root_node)
{
    long int c = 1;
    if (root_node == NULL)
    {
        return 0;
    }
    else
    {
        c += find_total_nodes(root_node->left_child);
        c += find_total_nodes(root_node->right_child);
        return c;
    }
}
long int find_tree_height_tmp(isax_node *root_node)
{

    if (root_node == NULL)
    {
        return -1;
    }
    else
    {
        long int left_depth = find_tree_height(root_node->left_child);
        long int right_depth = find_tree_height(root_node->right_child);

        if (left_depth > right_depth)
            return left_depth + 1;
        else
            return right_depth + 1;
    }
}

void *exact_search_worker_inmemory_hybridpqueue_ekosmas(void *rfdata)
{
    threadPin(((MESSI_workerdata_ekosmas *)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas *input_data = (MESSI_workerdata_ekosmas *)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    int workstealing_round = input_data->workstealing_round;

    query_result *bsf_result = input_data->bsf_result;
    int tnumber = rand() % NUM_PRIORITY_QUEUES; // is rand thread safe?
    int startqueuenumber = input_data->startqueuenumber;

    // A. Populate Queues
    // COUNT_QUEUE_TIME_START
    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_START
    }

    while (1)
    { // for all the subtrees
        int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);

        // work stealing on other node
        // when a node work-steal from the other node, processes i.e, 2000 subtrees and then goes to the next round
        if (simple_work_stealing && input_data->workstealing_on_which_tree_i_work == WORKSTEALING_WORK_ON_OTHER_TREE)
        {
            if (current_root_node_number >= workstealing_round * WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_OTHER_TREE)
            {
                break;
            }
            // node_position := array_len - ticket - 1
            current_root_node_number = input_data->amountnode - current_root_node_number - 1; // so we start from the end of the other node
        }
        // process a subset of subtrees from my tree index
        // for the work stealing technique
        // when a node processes its subtrees must process for example a subset of 10.000, so after that processing goes to the next round
        else if (simple_work_stealing && input_data->workstealing_on_which_tree_i_work == WORKSTEALING_WORK_ON_MY_TREE)
        {

            // processed already 2000 subtrees, so exit and go to the next "round"
            if (current_root_node_number >= workstealing_round * WORKSTEALING_NUM_OF_BUFFERS_TO_PROCESS_MY_TREE)
            {
                break;
            }
            // it for the final round is less than 2000 to process
            else if (current_root_node_number >= input_data->amountnode)
            {
                break;
            }
        }
        // ingnore this technique, each node build the whole dataset and then answer a query on the half of the subtrees
        else if (all_nodes_index_all_given_dataset == 1)
        {
            if (current_root_node_number >= (my_rank + 1) * input_data->amountnode / comm_sz)
            {
                break;
            }
        }
        // old code (without work stealing)
        else if (current_root_node_number >= input_data->amountnode)
        { // process_node_end // uncoment _HERE_
            break;
        }

        current_root_node = input_data->nodelist[current_root_node_number]; // use dinstance here!!!!
        insert_tree_node_m_hybridpqueue_ekosmas_tree_stats(paa, current_root_node, index, bsf_result /*bsfdisntance <-- ekosmas*/, input_data->allpq, input_data->alllock, &tnumber, input_data->workernumber, current_root_node_number);

        if (gather_subtree_stats)
        {
            subtree_stats[query_counter][current_root_node_number].distance_from_root = minidist_paa_to_isax(paa, current_root_node->isax_values,
                                                                                                             current_root_node->isax_cardinalities,
                                                                                                             index->settings->sax_bit_cardinality,
                                                                                                             index->settings->sax_alphabet_cardinality,
                                                                                                             index->settings->paa_segments,
                                                                                                             MINVAL, MAXVAL,
                                                                                                             index->settings->mindist_sqrt);

            subtree_stats[query_counter][current_root_node_number].BSF_of_tree = approximate_search_inmemory_pRecBuf_subtree_root_chatzakis(input_data->ts, paa, index, current_root_node);
            subtree_stats[query_counter][current_root_node_number].height = find_tree_height_tmp(current_root_node);
            subtree_stats[query_counter][current_root_node_number].total_nodes = find_total_nodes_tmp(current_root_node);
        }

        receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
    }

    // Wait all threads to fill in queues
    gettimeofday(&threads_start_time[input_data->workernumber], NULL);
    pthread_barrier_wait(input_data->lock_barrier);
    gettimeofday(&threads_curr_time[input_data->workernumber], NULL);

    // B. Processing my queue
    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    int checks = 0; // manolis: per thread // This is just for debugging. It can be removed!
    while (process_queue_node_ekosmas(input_data, startqueuenumber, &checks, input_data->workernumber))
    { // while a candidate queue node with smaller distance exists, compute actual distances
        receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
        ;
    }

    // C. Free any element left in my queue
    if ((input_data->allqueuelabel[startqueuenumber]) == 1)
    {
        (input_data->allqueuelabel[startqueuenumber]) = 0;
        pthread_mutex_lock(&(input_data->alllock[startqueuenumber]));
        query_result *n;
        while (n = pqueue_pop(input_data->allpq[startqueuenumber]))
        {
            share_bsf(input_data);
            free(n);
        }
        pthread_mutex_unlock(&(input_data->alllock[startqueuenumber]));
    }

    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // D. Process other uncompleted queues
    while (1) // ??? EKOSMAS: Why is this while(1) required?
    {
        bool finished = true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if ((input_data->allqueuelabel[i]) == 1)
            {
                finished = false;
                while (process_queue_node_ekosmas(input_data, i, &checks, input_data->workernumber))
                {
                    ;
                }
            }
        }

        if (finished)
        {
            break;
        }

        receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
    }

    // each thread at the end add the statistics to the global vars
    pthread_mutex_lock(&distances_counter_mutex);
    /* locked code */
    total_real_distance_counter += thread_real_distance_counter;
    total_min_distance_counter += thread_min_distance_counter;
    pthread_mutex_unlock(&distances_counter_mutex);

    pthread_barrier_wait(input_data->lock_barrier);
    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }
}
// ekosmas version
// traverseTree and add nodes to the queues ....
void insert_tree_node_m_hybridpqueue_ekosmas(float *paa, isax_node *node, isax_index *index, query_result *bsf_result /* float bsf <-- ekosmas*/, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber, int workernumber)
{
    // ??? EKOSMAS: Why not using SIMD version of the following function?
    float distance = minidist_paa_to_isax(paa, node->isax_values,
                                          node->isax_cardinalities,
                                          index->settings->sax_bit_cardinality,
                                          index->settings->sax_alphabet_cardinality,
                                          index->settings->paa_segments,
                                          MINVAL, MAXVAL,
                                          index->settings->mindist_sqrt);

    // check to get the bsf from other nodes.
    if (share_with_nodes_bsf)
    {
        if (workernumber == 0)
        { // only the thread with id 0.
            // change the bsf, do not lock because at this time all the other threads only read the bsf
            bsf_result->distance = try_to_receive_initial_bsfs();
        }
    }

    if (distance < bsf_result->distance)
    {
        if (node->is_leaf)
        {
            query_result *mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber = (*tnumber + 1) % NUM_PRIORITY_QUEUES;
        }
        else
        {
            if (node->left_child->isax_cardinalities != NULL) // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas(paa, node->left_child, index, bsf_result, pq, lock_queue, tnumber, workernumber);
            }
            if (node->right_child->isax_cardinalities != NULL) // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas(paa, node->right_child, index, bsf_result, pq, lock_queue, tnumber, workernumber);
            }
        }
    }
}

void insert_tree_node_m_hybridpqueue_chatzakis(float *paa, isax_node *node, isax_index *index, query_result *bsf_result /* float bsf <-- ekosmas*/, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber, int workernumber, int num_pqueues)
{
    float distance = minidist_paa_to_isax(paa, node->isax_values,
                                          node->isax_cardinalities,
                                          index->settings->sax_bit_cardinality,
                                          index->settings->sax_alphabet_cardinality,
                                          index->settings->paa_segments,
                                          MINVAL, MAXVAL,
                                          index->settings->mindist_sqrt);

    if (distance < bsf_result->distance)
    {
        if (node->is_leaf)
        {
            query_result *mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber = (*tnumber + 1) % num_pqueues;
        }
        else
        {
            if (node->left_child->isax_cardinalities != NULL) // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_chatzakis(paa, node->left_child, index, bsf_result, pq, lock_queue, tnumber, workernumber, num_pqueues);
            }
            if (node->right_child->isax_cardinalities != NULL) // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_chatzakis(paa, node->right_child, index, bsf_result, pq, lock_queue, tnumber, workernumber, num_pqueues);
            }
        }
    }
}

void insert_tree_node_m_hybridpqueue_ekosmas_tree_stats(float *paa, isax_node *node, isax_index *index, query_result *bsf_result /* float bsf <-- ekosmas*/, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber, int workernumber, int node_num)
{
    // ??? EKOSMAS: Why not using SIMD version of the following function?
    float distance = minidist_paa_to_isax(paa, node->isax_values,
                                          node->isax_cardinalities,
                                          index->settings->sax_bit_cardinality,
                                          index->settings->sax_alphabet_cardinality,
                                          index->settings->paa_segments,
                                          MINVAL, MAXVAL,
                                          index->settings->mindist_sqrt);

    if (gather_subtree_stats)
    {
        subtree_stats[query_counter][node_num].traversal_min_distance_calculations++;
    }

    // check to get the bsf from other nodes.
    if (share_with_nodes_bsf)
    {
        if (workernumber == 0)
        { // only the thread with id 0.
            // change the bsf, do not lock because at this time all the other threads only read the bsf
            bsf_result->distance = try_to_receive_initial_bsfs();
        }
    }

    if (distance < bsf_result->distance)
    {
        if (node->is_leaf)
        {
            query_result *mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber = (*tnumber + 1) % NUM_PRIORITY_QUEUES;

            if (gather_subtree_stats)
            {
                subtree_stats[query_counter][node_num].total_unpruned_nodes++; // only leafs...
            }
        }
        else
        {
            if (node->left_child->isax_cardinalities != NULL) // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas_tree_stats(paa, node->left_child, index, bsf_result, pq, lock_queue, tnumber, workernumber, node_num);
            }
            if (node->right_child->isax_cardinalities != NULL) // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas_tree_stats(paa, node->right_child, index, bsf_result, pq, lock_queue, tnumber, workernumber, node_num);
            }
        }
    }
}

/**
 * @brief Multi-thread safety
 *
 * @param rfdata
 * @return void*
 */
void *exact_search_worker_inmemory_hybridpqueue_parallel_chatzakis(void *rfdata)
{
    struct timeval t1, t2;
    double elapsedTime;

    // start timer
    gettimeofday(&t1, NULL);

    threadPin(((MESSI_workerdata_ekosmas *)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas *input_data = (MESSI_workerdata_ekosmas *)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    int workstealing_round = input_data->workstealing_round;
    int NUM_PRIORITY_QUEUES = input_data->num_prqueues; // overwrite the global scope.
    int queryID = input_data->queryID;
    pthread_mutex_t lock_min_real_distances = *(input_data->distances_lock_per_thread);

    query_result *bsf_result = input_data->bsf_result;
    int tnumber = rand() % NUM_PRIORITY_QUEUES; // is rand thread safe?
    int startqueuenumber = input_data->startqueuenumber;

    // A. Populate Queues
    // COUNT_QUEUE_TIME_START
    /*if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_START
    }*/

    while (1)
    { // for all the subtrees
        int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);

        if (all_nodes_index_all_given_dataset == 1)
        {
            if (current_root_node_number >= (my_rank + 1) * input_data->amountnode / comm_sz)
            {
                break;
            }
        }
        // old code (without work stealing)
        else if (current_root_node_number >= input_data->amountnode)
        { // process_node_end // uncoment _HERE_
            break;
        }

        current_root_node = input_data->nodelist[current_root_node_number]; // use dinstance here!!!!
        insert_tree_node_m_hybridpqueue_chatzakis(paa, current_root_node, index, bsf_result /*bsfdisntance <-- ekosmas*/, input_data->allpq, input_data->alllock, &tnumber, input_data->workernumber, input_data->num_prqueues);
    }

    // Wait all threads to fill in queues
    // gettimeofday(&threads_start_time[input_data->workernumber], NULL); //BIG RACE OVER HERE
    pthread_barrier_wait(input_data->lock_barrier);
    // gettimeofday(&threads_curr_time[input_data->workernumber], NULL); //BIG RACE OVER HERE

    // stop timer
    gettimeofday(&t2, NULL);

    // compute and print the elapsed time in millisec
    elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;    // sec to ms
    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    // printf("Similarity Search Func: Time needed of query %d by worker %d for tree traversal and pq fill is: %f seconds\n", queryID, input_data->workernumber, elapsedTime / 1000);

    gettimeofday(&t1, NULL);
    // B. Processing my queue
    /*if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }*/

    int checks = 0; // manolis: per thread // This is just for debugging. It can be removed!
    while (process_queue_node_ekosmas(input_data, startqueuenumber, &checks, input_data->workernumber))
    { // while a candidate queue node with smaller distance exists, compute actual distances
        ;
    }

    // C. Free any element left in my queue
    if ((input_data->allqueuelabel[startqueuenumber]) == 1)
    {
        (input_data->allqueuelabel[startqueuenumber]) = 0;
        pthread_mutex_lock(&(input_data->alllock[startqueuenumber]));
        query_result *n;
        while (n = pqueue_pop(input_data->allpq[startqueuenumber]))
        {
            // share_bsf(input_data);
            free(n);
        }
        pthread_mutex_unlock(&(input_data->alllock[startqueuenumber]));
    }

    /*if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }*/

    // D. Process other uncompleted queues
    while (1) // ??? EKOSMAS: Why is this while(1) required?
    {
        bool finished = true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if ((input_data->allqueuelabel[i]) == 1)
            {
                finished = false;
                while (process_queue_node_ekosmas(input_data, i, &checks, input_data->workernumber))
                {
                    ;
                }
            }
        }

        if (finished)
        {
            break;
        }
    }

    // each thread at the end add the statistics to the global vars
    // pthread_mutex_lock(&distances_counter_mutex); //old one
    // pthread_mutex_lock(&lock_min_real_distances);
    /* locked code */
    //*(input_data->total_real_distance_counter) += thread_real_distance_counter; //not sure if this is okay...
    //*(input_data->total_min_distance_counter) += thread_min_distance_counter; //not sure if this is okay...
    // pthread_mutex_unlock(&lock_min_real_distances);
    // pthread_mutex_unlock(&distances_counter_mutex); //old one

    /*if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }*/

    // stop timer
    gettimeofday(&t2, NULL);

    // compute and print the elapsed time in millisec
    elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;    // sec to ms
    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    // printf("Similarity Search Func: Time needed of query %d by worker %d for pq processing is: %f seconds\n", queryID, input_data->workernumber, elapsedTime / 1000);
}

// ############################### WORK STEALING METHODS ###############################

// this function is called to process a chunk of the subtrees
// first 10.000 subtrees, next 10.000 subtrees etc ...
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_simple_workstealing_manol(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                                                                                     float minimum_distance, int node_counter_fetch_add_start_value, int workstealing_round, int specify_working_tree, int compute_initial_bsf)
{
    // Does not calculate initial bsf, it will use the minimum_distance
    query_result bsf_result;
    if (simple_work_stealing && compute_initial_bsf)
    {
        bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index); // what is this a stack variable ???
        SET_APPROXIMATE(bsf_result.distance);
        bsf_result.init_bsf = bsf_result.distance;
        if (share_with_nodes_bsf)
        {
            share_initial_bsf(bsf_result);
        }
    }
    else
    {
        bsf_result.distance = minimum_distance;
    }

    // Early termination...
    if (bsf_result.distance == 0)
    {
        return bsf_result;
    }

    if (maxquerythread == 1)
    {
        NUM_PRIORITY_QUEUES = 1;
    }
    else
    {
        NUM_PRIORITY_QUEUES = maxquerythread / 2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t *) * NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES]; // EKOSMAS, 21 SEPTEMBER 2020: REMOVED

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER; // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size / NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i] = 1;
    }

    volatile int node_counter = node_counter_fetch_add_start_value; // EKOSMAS, 29 AUGUST 2020: added volatile
    // 26-02-2021
    // node ammount / 2 set with appropriate value
    // procces_node end and pass it into input data

    // how many subtrees must a node process
    if (all_nodes_index_all_given_dataset == 1)
    {
        node_counter = my_rank * (nodelist->node_amount / comm_sz);
    }

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                            // query ts
        workerdata[i].paa = paa;                          // query paa
        workerdata[i].lock_bsf = &lock_bsf;               // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;         // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount; // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;        // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;               // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                   // the BSF
        workerdata[i].lock_barrier = &lock_barrier;               // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                        // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                 // ??? How is this used?
        workerdata[i].allpq = allpq;                              // priority queues
        workerdata[i].startqueuenumber = i % NUM_PRIORITY_QUEUES; // initial priority queue to start
        workerdata[i].workernumber = i;

        workerdata[i].workstealing_round = workstealing_round;
        workerdata[i].workstealing_on_which_tree_i_work = specify_working_tree;
    }
    // check if there is arrive a bsf message from the other nodes and change the bsf related to prunning
    if (share_with_nodes_bsf)
    {
        bsf_result.distance = try_to_receive_initial_bsfs();
    }

    // threads creations
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas, (void *)&(workerdata[i]));
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    update_threads_statistics();
    pthread_barrier_destroy(&lock_barrier);

    query_counter++;

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    return bsf_result;
    // Free the nodes that where not popped.
}

// CHATZAKIS

void *exact_search_worker_inmemory_hybridpqueue_dynamic_module_chatzakis(void *rfdata)
{
    threadPin(((MESSI_workerdata_ekosmas *)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas *input_data = (MESSI_workerdata_ekosmas *)rfdata;
    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    int workstealing_round = input_data->workstealing_round;

    communication_module_data *comm_data = input_data->comm_data;

    query_result *bsf_result = input_data->bsf_result;
    int tnumber = rand() % NUM_PRIORITY_QUEUES; // is rand thread safe?
    int startqueuenumber = input_data->startqueuenumber;

    volatile int *threads_finished = input_data->threads_finished;

    // A. Populate Queues
    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_START
    }

    while (1)
    { // for all the subtrees
        int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);

        if (current_root_node_number >= input_data->amountnode)
        {
            break;
        }

        current_root_node = input_data->nodelist[current_root_node_number]; // use dinstance here!!!!
        insert_tree_node_m_hybridpqueue_ekosmas(paa, current_root_node, index, bsf_result, input_data->allpq, input_data->alllock, &tnumber, input_data->workernumber);

        // share_bsf_chatzakis(bsf_result, input_data->workernumber);
        receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
    }

    if (comm_data != NULL && input_data->workernumber == 0 && comm_data->mode == MODULE)
    {
        CALL_MODULE
    }

    // Wait all threads to fill in queues
    gettimeofday(&threads_start_time[input_data->workernumber], NULL);
    pthread_barrier_wait(input_data->lock_barrier);
    gettimeofday(&threads_curr_time[input_data->workernumber], NULL);

    // B. Processing my queue
    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    int checks = 0; // manolis: per thread // This is just for debugging. It can be removed!
    while (process_queue_node_chatzakis(input_data, startqueuenumber, &checks, input_data->workernumber, comm_data))
    { // while a candidate queue node with smaller distance exists, compute actual distances
        ;

        if (comm_data != NULL && input_data->workernumber == 0 && comm_data->mode == MODULE)
        {
            CALL_MODULE
        }
    }

    // C. Free any element left in my queue
    if ((input_data->allqueuelabel[startqueuenumber]) == 1)
    {
        (input_data->allqueuelabel[startqueuenumber]) = 0;
        pthread_mutex_lock(&(input_data->alllock[startqueuenumber]));
        query_result *n;
        while (n = pqueue_pop(input_data->allpq[startqueuenumber]))
        {
            share_bsf(input_data);
            free(n);
        }
        pthread_mutex_unlock(&(input_data->alllock[startqueuenumber]));
    }

    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // D. Process other uncompleted queues
    while (1) // ??? EKOSMAS: Why is this while(1) required?
    {
        bool finished = true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if ((input_data->allqueuelabel[i]) == 1)
            {
                finished = false;
                while (process_queue_node_ekosmas(input_data, i, &checks, input_data->workernumber))
                {
                    // share_bsf_chatzakis(input_data->bsf_result, input_data->workernumber);
                    receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->lock_bsf);
                }
            }
        }

        if (finished)
        {
            break;
        }
    }

    // each thread at the end add the statistics to the global vars
    pthread_mutex_lock(&distances_counter_mutex);
    /* locked code */
    total_real_distance_counter += thread_real_distance_counter;
    total_min_distance_counter += thread_min_distance_counter;
    pthread_mutex_unlock(&distances_counter_mutex);

    if (input_data->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }
}

typedef struct coordinator_data
{
    communication_module_data *comm_data;
    volatile char *threads_finished;
} coordinator_data;
void *coordinator_thread_module_handling_chatzakis(void *rfdata)
{
    coordinator_data *data = (coordinator_data *)rfdata;

    communication_module_data *comm_data = data->comm_data;
    volatile char *threads_finished = data->threads_finished;

    // printf("Module handler thread started...\n");

    while (!(*threads_finished))
    {
        if (comm_data != NULL) // just for safety
        {
            CALL_MODULE
        }
    }

    pthread_exit(NULL);
}

query_result exact_search_ParISnew_inmemory_hybrid_parallel_chatzakis(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist, float minimum_distance, int queryNum, unsigned long long int *min_dists, unsigned long long int *real_dists)
{
    // debug
    struct timeval t1, t2;
    double elapsedTime;
    gettimeofday(&t1, NULL);

    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);

    queries_statistics_manol[queryNum].initial_bsf = bsf_result.distance;

    if (bsf_result.distance == 0)
    {
        return bsf_result;
    }

    int NUM_PRIORITY_QUEUES;
    if (maxquerythread == 1)
    {
        NUM_PRIORITY_QUEUES = 1;
    }
    else
    {
        NUM_PRIORITY_QUEUES = maxquerythread / 2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t *) * NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES]; // EKOSMAS, 21 SEPTEMBER 2020: REMOVED

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER; // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    /*Distance Locks*/
    pthread_mutex_t lock_min_real_distances = PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size / NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i] = 1;
    }

    volatile int node_counter = 0; // EKOSMAS, 29 AUGUST 2020: added volatile

    // how many subtrees must a node process
    if (all_nodes_index_all_given_dataset == 1)
    {
        node_counter = my_rank * (nodelist->node_amount / comm_sz);
    }

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                            // query ts
        workerdata[i].paa = paa;                          // query paa
        workerdata[i].lock_bsf = &lock_bsf;               // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;         // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount; // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;        // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;               // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                   // the BSF
        workerdata[i].lock_barrier = &lock_barrier;               // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                        // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                 // ??? How is this used?
        workerdata[i].allpq = allpq;                              // priority queues
        workerdata[i].startqueuenumber = i % NUM_PRIORITY_QUEUES; // initial priority queue to start
        workerdata[i].workernumber = i;

        workerdata[i].workstealing_round = -1;
        workerdata[i].workstealing_on_which_tree_i_work = WORKSTEALING_WORK_ON_MY_TREE;

        workerdata[i].num_prqueues = NUM_PRIORITY_QUEUES;
        workerdata[i].distances_lock_per_thread = &lock_min_real_distances;

        workerdata[i].total_min_distance_counter = min_dists;
        workerdata[i].total_real_distance_counter = real_dists;

        workerdata[i].queryID = queryNum;
    }

    gettimeofday(&t2, NULL);
    // compute and print the elapsed time in millisec
    elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;    // sec to ms
    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    // printf("Time needed for initializing the perthread search data for query %d is %f s.\n", queryNum, elapsedTime / 1000);

    gettimeofday(&t1, NULL);
    // printf("[MC] ----------------------------------------- Search Threads: %d\n", maxquerythread);
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_parallel_chatzakis, (void *)&(workerdata[i]));
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    update_threads_statistics();
    pthread_barrier_destroy(&lock_barrier);

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    gettimeofday(&t2, NULL);
    // compute and print the elapsed time in millisec
    elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;    // sec to ms
    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
    // printf("Time needed for exact query answerting of the perthread search data for query %d is %f s.\n", queryNum, elapsedTime / 1000);

    return bsf_result;
}

query_result exact_search_ParISnew_inmemory_hybrid_no_approximate_calc_chatzakis(dress_query *d_query, ts_type *paa, isax_index *index, node_list *nodelist, float minimum_distance, communication_module_data *comm_data)
{
    query_result bsf_result = d_query->initialBSF;

    queries_statistics_manol[d_query->id].initial_bsf = bsf_result.distance;

    // Early termination...
    if (bsf_result.distance == 0)
    {
        query_counter++;
        return bsf_result;
    }

    if (maxquerythread == 1)
    {
        NUM_PRIORITY_QUEUES = 1;
    }
    else
    {
        NUM_PRIORITY_QUEUES = maxquerythread / 2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t *) * NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES]; // EKOSMAS, 21 SEPTEMBER 2020: REMOVED

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER; // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size / NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i] = 1;
    }

    volatile int node_counter = 0; // EKOSMAS, 29 AUGUST 2020: added volatile
    // 26-02-2021
    // node ammount / 2 set with appropriate value
    // procces_node end and pass it into input data

    // how many subtrees must a node process
    if (all_nodes_index_all_given_dataset == 1)
    {
        node_counter = my_rank * (nodelist->node_amount / comm_sz);
    }

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = d_query->query;                // query ts
        workerdata[i].paa = paa;                          // query paa
        workerdata[i].lock_bsf = &lock_bsf;               // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;         // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount; // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;        // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;               // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                   // the BSF
        workerdata[i].lock_barrier = &lock_barrier;               // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                        // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                 // ??? How is this used?
        workerdata[i].allpq = allpq;                              // priority queues
        workerdata[i].startqueuenumber = i % NUM_PRIORITY_QUEUES; // initial priority queue to start
        workerdata[i].workernumber = i;

        workerdata[i].workstealing_round = -1;
        workerdata[i].workstealing_on_which_tree_i_work = WORKSTEALING_WORK_ON_MY_TREE;

        workerdata[i].comm_data = comm_data;
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_dynamic_module_chatzakis, (void *)&(workerdata[i]));
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    update_threads_statistics();

    pthread_barrier_destroy(&lock_barrier);

    query_counter++;

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    return bsf_result;
}

query_result exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis(search_function_params args)
{
    static int first_time_flag = 1;

    ts_type *ts = args.ts;
    ts_type *paa = args.paa;
    isax_index *index = args.index;
    node_list *nodelist = args.nodelist;
    communication_module_data *comm_data = args.comm_data;
    float minimum_distance = args.minimum_distance;

    float *shared_bsf_results = args.shared_bsf_results;

    query_counter = args.query_id;

    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);

    SET_APPROXIMATE(bsf_result.distance);

    queries_statistics_manol[query_counter].initial_bsf = bsf_result.distance; // MC: This is a bug for distributed context coming from manolis code, should be solved if query_counter is passed as an arg from calling method

    if (bsf_sharing_pdr)
    {
        if (first_time_flag)
        {
            share_initial_bsf_chatzakis(bsf_result);
            first_time_flag = 0;
        }
        else
        {
            share_bsf_chatzakis_parent_func(&bsf_result);
        }
    }

    if (bsf_result.distance == 0)
    {
        return bsf_result;
    }

    if (maxquerythread == 1)
    {
        NUM_PRIORITY_QUEUES = 1;
    }
    else
    {
        NUM_PRIORITY_QUEUES = maxquerythread / 2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t *) * NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES]; // EKOSMAS, 21 SEPTEMBER 2020: REMOVED

    pthread_t threadid[maxquerythread];

    // Chatzakis Coordinator Patch
    pthread_t coordinator_thread_id;
    char threads_finished = 0;

    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER; // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size / NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i] = 1;
    }

    volatile int node_counter = 0;
    // how many subtrees must a node process
    /*if (all_nodes_index_all_given_dataset == 1)
    {
        node_counter = my_rank * (nodelist->node_amount / comm_sz);
    }*/

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                            // query ts
        workerdata[i].paa = paa;                          // query paa
        workerdata[i].lock_bsf = &lock_bsf;               // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;         // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount; // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;        // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;               // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                   // the BSF
        workerdata[i].lock_barrier = &lock_barrier;               // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                        // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                 // ??? How is this used?
        workerdata[i].allpq = allpq;                              // priority queues
        workerdata[i].startqueuenumber = i % NUM_PRIORITY_QUEUES; // initial priority queue to start
        workerdata[i].workernumber = i;

        workerdata[i].workstealing_round = -1;
        workerdata[i].workstealing_on_which_tree_i_work = WORKSTEALING_WORK_ON_MY_TREE;

        // Chatzakis additions
        workerdata[i].comm_data = comm_data;
        workerdata[i].shared_bsf_results = shared_bsf_results;
    }

    if (bsf_sharing_pdr)
    {
        float candidate_bsf = try_to_receive_initial_bsfs_chatzakis(shared_bsf_results);

        if (shared_bsf_results[query_counter] < candidate_bsf)
        {
            // printf("YES YES YES YES YES YES YES YES\n");
            if (ENABLE_PRINTS_BSF_SHARING)
            {
                printf("[BSF SHARING BOOK-KEEPING] Node %d starts with answer kept = %f for query %d, initial bsf was %f\n", my_rank, shared_bsf_results[query_counter], query_counter, bsf_result.distance);
            }
            candidate_bsf = shared_bsf_results[query_counter];
        }

        bsf_result.distance = candidate_bsf;
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_dynamic_module_chatzakis, (void *)&(workerdata[i]));
    }

    if (comm_data != NULL && comm_data->mode == THREAD)
    {
        coordinator_data c_data;
        c_data.comm_data = comm_data;
        c_data.threads_finished = &threads_finished;
        pthread_create(&coordinator_thread_id, NULL, coordinator_thread_module_handling_chatzakis, (void *)&c_data);
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    // printf("[MC] - Query Workers Finished...\n");
    threads_finished = 1;

    if (comm_data != NULL && comm_data->mode == THREAD)
    {
        pthread_join(coordinator_thread_id, NULL);
    }

    // printf("[MC] - Module Handler Finished...\n");

    update_threads_statistics();
    pthread_barrier_destroy(&lock_barrier);

    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    return bsf_result;
}

// ! -------------- WORKSTEALING - NEW VERSION - CHATZAKIS --------------
extern int workstealing_chatzakis;

typedef struct workstealing_thread_data
{
    volatile char *query_workers_finished;
    volatile char *priority_queues_filled;
    volatile char *receiving_workstealing;

    pthread_mutex_t *bsf_lock;
    query_result *bsf_result;

    batch_list *batchlist;

} workstealing_thread_data;

void process_subtree_node_insert_pq_chatzakis(isax_node *subtree_node, subtree_batch *batch /*int batch_index*/, float bsf_distance, ts_type *paa, isax_index *index)
{
    // subtree_batch *batch = &batches[batch_index];

    if (subtree_node == NULL || subtree_node->isax_values == NULL || subtree_node->isax_cardinalities == NULL)
    {
        return;
    }

    float distance = minidist_paa_to_isax(paa, subtree_node->isax_values,
                                          subtree_node->isax_cardinalities,
                                          index->settings->sax_bit_cardinality,
                                          index->settings->sax_alphabet_cardinality,
                                          index->settings->paa_segments,
                                          MINVAL, MAXVAL,
                                          index->settings->mindist_sqrt);

    if (distance < bsf_distance)
    {
        if (subtree_node->is_leaf)
        {
            query_result *mindist_result = malloc(sizeof(query_result));
            mindist_result->node = subtree_node;
            mindist_result->distance = distance;

            pthread_mutex_lock(&batch->pq_insert_lock);

            // !MADE A PATCH OPTIMIZATION -- MAY CAUSE PROBLEM
            if (batch->pq[batch->pq_amount] == NULL)
            {
                batch->pq[batch->pq_amount] = pqueue_init(batch->pq_th, cmp_pri, get_pri, set_pri, get_pos, set_pos);
                batch->pq[batch->pq_amount]->is_stolen = 0;
                batch->pq[batch->pq_amount]->batch_id = batch->id;
            }

            pqueue_insert(batch->pq[batch->pq_amount], mindist_result);

            if ((batch->pq[batch->pq_amount])->size >= batch->pq_th)
            {
                batch->pq_amount++;

                if (batch->pq_amount == MAX_PQs_WORKSTEALING)
                {
                    printf("ERROR::: Priority queues per batch exceeded the limit of %d. Increase the limit. Exiting...\n", MAX_PQs_WORKSTEALING);
                    exit(EXIT_FAILURE);
                }

                // batch->pq[++batch->pq_amount] = pqueue_init(batch->pq_th, cmp_pri, get_pri, set_pri, get_pos, set_pos);
                // batch->pq[batch->pq_amount]->is_stolen = 0;        // for workstealing
                // batch->pq[batch->pq_amount]->batch_id = batch->id; // for workstealing
            }

            pthread_mutex_unlock(&batch->pq_insert_lock);
        }
        else
        {
            process_subtree_node_insert_pq_chatzakis(subtree_node->right_child, batch, bsf_distance, paa, index);
            process_subtree_node_insert_pq_chatzakis(subtree_node->left_child, batch, bsf_distance, paa, index);
        }
    }
}

void process_batch_chatzakis(int batch_index, subtree_batch *batches, float bsf_distance, isax_index *index, ts_type *paa)
{
    int subtree_index;
    while (1)
    {
        subtree_index = __sync_fetch_and_add(&batches[batch_index].current_subtree_to_process, 1); // check the param again?

        if (subtree_index >= batches[batch_index].size)
        {
            break;
        }

        isax_node *subtree_node = (batches[batch_index].nodelist->nlist)[subtree_index + batches[batch_index].from];
        // printf("Processing subtree %d\n", subtree_index);

        process_subtree_node_insert_pq_chatzakis(subtree_node, &batches[batch_index] /*batch_index*/, bsf_distance, paa, index);
    }

    return;
}

void receive_shared_bsf_chatzakis_v(MESSI_workstealing_query_data_chatzakis *input_data)
{
    if (workstealing_share_bsf == 0 || *(input_data->receiving_workstealing) == 0 || input_data->workernumber != 0 /*|| my_rank != 1*/)
    {
        return;
    }

    int ready;

    for (int rank = 0; rank < comm_sz; rank++)
    {
        if (rank == my_rank)
        {
            continue;
        }

        COUNT_MPI_TIME_START
        MPI_Test(&input_data->bsf_receive_requests[rank], &ready, MPI_STATUS_IGNORE);
        COUNT_MPI_TIME_END

        if (ready)
        {

            int query_number = (int)input_data->bsf_received_data[0];
            float new_bsf = input_data->bsf_received_data[1];

            pthread_mutex_lock(input_data->bsf_lock);
            if ((input_data->bsf_result)->distance > new_bsf)
            {
                bsf_change_counter++;
                printf("[ -- WORKS _ _ _ _ _ _ ]=> Node %d received a new BSF Value from node %d, val = [%d, %f]\n", my_rank, rank, query_number, new_bsf);
                (input_data->bsf_result)->distance = new_bsf;
            }
            pthread_mutex_unlock(input_data->bsf_lock);

            COUNT_COMMUNICATION_TIME_START
            COUNT_MPI_TIME_START
            MPI_Irecv(input_data->bsf_received_data, 2, MPI_FLOAT, rank, WORKSTEALING_BSF_SHARE, MPI_COMM_WORLD, &input_data->bsf_receive_requests[rank]);
            COUNT_MPI_TIME_END
            COUNT_COMMUNICATION_TIME_END
        }
    }
}

void share_bsf_chatzakis_v(MESSI_workstealing_query_data_chatzakis *input_data)
{
    if (workstealing_share_bsf == 0 || *(input_data->receiving_workstealing) == 0)
    {
        return;
    }

    for (int rank = 0; rank < comm_sz; rank++)
    {
        if (rank == my_rank)
        {
            continue;
        }

        float bsf_data[2] = {query_counter * 1.0, input_data->bsf_result->distance /*, -1.0*/};
        // printf("QUERY_COUNTER =========== %d\n", query_counter);
        // printf("[WORKSTEALING BSF SHARING SEND]: Node %d send to node %d a new improved BSF=[%f,%f]\n", my_rank, rank, bsf_data[0], bsf_data[1]);
        COUNT_MPI_TIME_START
        MPI_Send(bsf_data, 2, MPI_FLOAT, rank, WORKSTEALING_BSF_SHARE, MPI_COMM_WORLD);
        COUNT_MPI_TIME_END
    }
}

int process_pq_of_batch_chatzakis(int current_pq_index, MESSI_workstealing_query_data_chatzakis *input_data)
{
    pqueue_t *pq = (*(input_data->final_pq_list))[current_pq_index];

    query_result *n = pqueue_pop(pq);

    share_bsf_chatzakis(input_data->bsf_result, input_data->workernumber);
    receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->bsf_lock);

    if (n == NULL)
    {
        return 0;
    }

    float current_bsf_dist = input_data->bsf_result->distance;

    if (n->distance > current_bsf_dist || n->distance > input_data->minimum_distance)
    {
        return 0;
    }

    if (n->node->is_leaf)
    {
        float distance = calculate_node_distance2_inmemory_chatzakis(input_data->index, n->node, input_data->ts, input_data->paa, current_bsf_dist, input_data, input_data->bsf_result);

        receive_shared_bsf_chatzakis(input_data->workernumber, input_data->bsf_result, input_data->shared_bsf_results, input_data->bsf_lock);
        share_bsf_chatzakis(input_data->bsf_result, input_data->workernumber);

        if (distance < input_data->bsf_result->distance)
        {
            pthread_mutex_lock(input_data->bsf_lock);

            bsf_change_counter++;

            input_data->bsf_result->distance = distance;
            input_data->bsf_result->node = n->node;

            share_bsf_chatzakis(input_data->bsf_result, input_data->workernumber);
            //  printf("New BSF: %f FROM PQ %d\n", distance, current_pq_index);

            communication_module_data *comm_data = input_data->comm_data;
            if (comm_data != NULL && input_data->workernumber == 0 && comm_data->mode == MODULE)
            {
                CALL_MODULE
            }

            pthread_mutex_unlock(input_data->bsf_lock);
        }
    }

    free(n);

    return 1;
}

void process_pqs_of_batch_chatzakis(subtree_batch *batch, MESSI_workstealing_query_data_chatzakis *input_data)
{
    int current_pq_to_process_index;
    while (1)
    {

        current_pq_to_process_index = __sync_fetch_and_add(&batch->current_pq_to_process, 1);

        if (current_pq_to_process_index >= batch->pq_amount)
        {
            break;
        }

        // process_pq_of_batch_chatzakis((batch->pq)[batch->pq_amount], input_data);
    }
}

extern MPI_Request *global_helper_requests;
int r;
void *workstealing_thread_routine_chatzakis(void *rfdata)
{
    workstealing_thread_data *data = (workstealing_thread_data *)rfdata;
    volatile char *threads_finished = data->query_workers_finished;
    static int first_time = 1;

    // MPI_Request recv_request[comm_sz]; //! make static to keep old data
    int recv_message;
    int ready;

    int coordinator_of_current_group_rank = FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes);
    if (first_time)
    {

        for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
        {
            if (rank == my_rank)
            {
                continue;
            }

            MPI_Irecv(&r, 1, MPI_INT, rank, WORKSTEALING_INFORM_AVAILABILITY, MPI_COMM_WORLD, &global_helper_requests[rank]);
        }

        first_time = 0;
    }

    while ((*threads_finished) == 0)
    {
        for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
        {
            if (rank == my_rank)
            {
                continue;
            }

            MPI_Test(&global_helper_requests[rank], &ready, MPI_STATUS_IGNORE);

            if (ready)
            {
                if (ENABLE_PRINTS_WORKSTEALING)
                {
                    printf("[WORKSTEALING MAIN NODE] - Workstealing thread of node %d found that node %d can help!\n", my_rank, rank);
                }

                *(data->receiving_workstealing) = 1;

                while (!(*data->priority_queues_filled))
                {
                    // busy wait if the pqs are not yet filled
                    ;
                }

                // find the batch with the longest min
                int batch_ids_to_send[batches_to_send];
                int are_batches_available = 1;
                // printf("BA: %d\n", (data->batchlist)->batch_amount);
                for (int i = 0; i < batches_to_send; i++)
                {
                    int selected_batch_id = -1;
                    int farest_min = 0;
                    for (int b = 0; b < (data->batchlist)->batch_amount; b++)
                    {
                        if ((data->batchlist)->batches[b].is_stolen || (data->batchlist)->batches[b].pq[0] == NULL)
                        {
                            continue;
                        }

                        int current_min = (data->batchlist)->batches[b].min_pq_index;
                        // printf("TH --- Current min=%d\n", current_min);
                        if (current_min >= farest_min)
                        {
                            farest_min = current_min;
                            selected_batch_id = b;
                        }
                    }

                    if (selected_batch_id == -1)
                    {
                        are_batches_available = 0;
                        break;
                    }
                    else
                    {
                        batch_ids_to_send[i] = selected_batch_id;
                        (data->batchlist)->batches[selected_batch_id].is_stolen = 1;
                    }
                }

                // printf("selected a batch to send = %d...\n", selected_batch_id);
                if (are_batches_available)
                {
                    for (int id = 0; id < batches_to_send; id++)
                    {
                        for (int i = 0; i < (data->batchlist)->batches[batch_ids_to_send[id]].pq_amount + 1; i++)
                        {
                            if ((data->batchlist)->batches[batch_ids_to_send[id]].pq[i] != NULL && (data->batchlist)->batches[batch_ids_to_send[id]].pq[i]->size > 1)
                            {
                                (data->batchlist)->batches[batch_ids_to_send[id]].pq[i]->is_stolen = 1;
                            }
                        }
                    }

                    float bsf = (data->bsf_result)->distance;
                    float datas[2 + batches_to_send];
                    datas[0] = query_counter;
                    datas[1] = bsf;
                    for (int i = 0; i < batches_to_send; i++)
                    {
                        datas[2 + i] = batch_ids_to_send[i];
                    }
                    if (ENABLE_PRINTS_WORKSTEALING)
                    {
                        printf("[WORKSTEALING MAIN NODE] - Node %d sending message to node %d :  [ %f,%f, ", my_rank, rank, datas[0], datas[1]);
                        for (int t = 0; t < batches_to_send; t++)
                        {
                            printf("%f ", datas[t + 2]);
                        }
                        printf("] .\n");
                    }

                    MPI_Send(datas, 2 + batches_to_send, MPI_FLOAT, rank, WORKSTEALING_DATA_SEND, MPI_COMM_WORLD);
                }
                else
                {
                    // printf("[WORKSTEALING MAIN NODE] - Workstealing thread of node %d send to node %d: [TERM], as it didnt find a batch to send\n", my_rank, rank);
                    float datas[2 + batches_to_send]; //! complete the rest of the array
                    datas[0] = -1;
                    MPI_Send(datas, 2 + batches_to_send, MPI_FLOAT, rank, WORKSTEALING_DATA_SEND, MPI_COMM_WORLD);
                }

                MPI_Irecv(&recv_message, 1, MPI_INT, rank, WORKSTEALING_INFORM_AVAILABILITY, MPI_COMM_WORLD, &global_helper_requests[rank]);
            }
        }
    }

    // Announcing that main node completed the computation of this query.
    /*for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    {
        if (my_rank != rank)
        {
            float datas[2 + batches_to_send];
            datas[0] = -1;
            MPI_Send(datas, 2 + batches_to_send, MPI_FLOAT, rank, WORKSTEALING_DATA_SEND, MPI_COMM_WORLD);
        }
    }*/

    pthread_exit(NULL);
}

void My_threadPin(int pid, int max_threads)
{
    int cpu_id;

    cpu_id = pid % max_threads;
    pthread_setconcurrency(max_threads);

    cpu_set_t mask;
    unsigned int len = sizeof(mask);

    CPU_ZERO(&mask);

    CPU_SET(cpu_id % max_threads, &mask); // OLD PINNING 1

    // if (cpu_id % 2 == 0)                                             // OLD PINNING 2
    //    CPU_SET(cpu_id % max_threads, &mask);
    // else
    //    CPU_SET((cpu_id + max_threads/2)% max_threads, &mask);

    // if (cpu_id % 2 == 0)                                             // FULL HT
    //    CPU_SET(cpu_id/2, &mask);
    // else
    //    CPU_SET((cpu_id/2) + (max_threads/2), &mask);

    // CPU_SET((cpu_id%4)*10 + (cpu_id%40)/4 + (cpu_id/40)*40, &mask);     // SOCKETS PINNING - Vader

    int ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");
}

int compare_pqs(pqueue_t *q1, pqueue_t *q2)
{
    query_result *dist1 = pqueue_peek(q1);
    query_result *dist2 = pqueue_peek(q2);

    return !(dist1->distance > dist2->distance);
}

void *exact_search_workstealing_thread_routine_chatzakis(void *rfdata)
{
    My_threadPin(((MESSI_workstealing_query_data_chatzakis *)rfdata)->workernumber, maxquerythread);

    MESSI_workstealing_query_data_chatzakis *in_data = (MESSI_workstealing_query_data_chatzakis *)rfdata;
    communication_module_data *comm_data = in_data->comm_data;

    if (in_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_START
    }

    int current_batch_index;
    while (1)
    {
        current_batch_index = __sync_fetch_and_add(in_data->batch_counter, 1);

        if (current_batch_index >= in_data->total_batches)
        {
            break;
        }

        if (comm_data != NULL && in_data->workernumber == 0 && comm_data->mode == MODULE)
        {
            CALL_MODULE
        }

        process_batch_chatzakis(current_batch_index, in_data->batches, in_data->bsf_result->distance, in_data->index, in_data->paa);
        (in_data->batches)[current_batch_index].processed_phase_1 = 1;

        receive_shared_bsf_chatzakis(in_data->workernumber, in_data->bsf_result, in_data->shared_bsf_results, in_data->bsf_lock);
    }

    // printf("[MC] - Thread %d finished processing the buffers and proceeds to help another thread.\n", in_data->workernumber);

    for (int i = 0; i < in_data->total_batches; i++)
    {
        if (!(in_data->batches)[i].processed_phase_1 && !(in_data->batches)[i].is_getting_help_phase1)
        {
            (in_data->batches)[i].is_getting_help_phase1 = 1;
            process_batch_chatzakis(i, in_data->batches, in_data->bsf_result->distance, in_data->index, in_data->paa);
            receive_shared_bsf_chatzakis(in_data->workernumber, in_data->bsf_result, in_data->shared_bsf_results, in_data->bsf_lock);
        }
    }

    // printf("[MC] - Thread %d finished helping, waiting at the phase 1 barrier.\n", in_data->workernumber);
    pthread_barrier_wait(in_data->sync_barrier);
    if (in_data->workernumber == 0)
    {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_PQ_PREPROCESSING_TIME_START
    }

    pqueue_t **all_pqs;
    if (in_data->workernumber == 0)
    {
        /*for (int i = 0; i < in_data->total_batches; i++)
        {
            subtree_batch batch = in_data->batches[i];
            printf("[MC] - Subtree batch %d: PQ_AMOUNT: %d ", i, batch.pq_amount);
            int total_pqs = batch.pq_amount + 1;
            for (int j = 0; j < total_pqs; j++)
            {
                if (batch.pq[j] != NULL)
                {
                    printf("pq[%d].size = %d ", j, batch.pq[j]->size);
                }
            }
            printf("\n");
        }*/
        int total_pqs = 0;
        for (int i = 0; i < in_data->total_batches; i++)
        {
            int pqs = in_data->batches[i].pq_amount + 1;
            for (int j = 0; j < pqs; j++)
            {
                if (in_data->batches[i].pq[j] != NULL && in_data->batches[i].pq[j]->size > 1)
                {
                    total_pqs++;
                }
            }
        }

        *(in_data->final_pq_list_size) = total_pqs;
        *(in_data->final_pq_list) = (pqueue_t **)malloc(sizeof(pqueue_t *) * total_pqs);

        int curr_pq = 0;
        for (int i = 0; i < in_data->total_batches; i++)
        {
            int pqs = in_data->batches[i].pq_amount + 1;
            for (int j = 0; j < pqs; j++)
            {
                if (in_data->batches[i].pq[j] != NULL && in_data->batches[i].pq[j]->size > 1)
                {
                    in_data->batches[i].pq[j]->batch_id = i;
                    (*(in_data->final_pq_list))[curr_pq++] = in_data->batches[i].pq[j];
                }
            }
        }

        // shuffle_pq_array(total_pqs, *(in_data->final_pq_list)); //shuffling shown minor improvements

        /*for (int i = 0; i < *(in_data->final_pq_list_size); i++)
        {
            // printf("PQ[%d]_peek = %f\n", i, ((query_result *)pqueue_peek((*(in_data->final_pq_list))[i]))->distance);
            printf("pq[%d].(size,peak) = (%d,%f) ", i, (*(in_data->final_pq_list))[i]->size, ((query_result *)pqueue_peek((*(in_data->final_pq_list))[i]))->distance);
        }
        printf("\n");
        */

        if (gather_pq_stats)
        {
            fprintf(pq_stats_file, "%d %f %d %d ", query_counter, in_data->bsf_result->distance, in_data->batches[0].pq_th, total_pqs);

            for (int i = 0; i < total_pqs; i++)
            {
                fprintf(pq_stats_file, "%d ", (*(in_data->final_pq_list))[i]->size);
            }
            fprintf(pq_stats_file, "\n");
        }

        sort_pqs_chatzakis(total_pqs, *(in_data->final_pq_list), compare_pqs);

        for (int pq_num = 0; pq_num < total_pqs; pq_num++)
        {
            pqueue_t *p = (*(in_data->final_pq_list))[pq_num];
            subtree_batch *belonging_batch = &(in_data->batches)[p->batch_id];

            if (belonging_batch->min_pq_index > pq_num)
            {
                belonging_batch->min_pq_index = pq_num;
            }

            if (belonging_batch->max_pq_index < pq_num)
            {
                belonging_batch->max_pq_index = pq_num;
            }
        }

        // printf("Array size %d\n", total_pqs);
        /*for (int i = 0; i < in_data->total_batches; i++)
        {
            if (in_data->batches[i].pq_amount == 0 && in_data->batches[i].pq[0] == NULL)
            {
                continue;
            }

            printf("Batch[%d] (num_pqs = %d) min = %d, max= %d\n", i, in_data->batches[i].pq_amount + 1, in_data->batches[i].min_pq_index, in_data->batches[i].max_pq_index);
        }*/

        *(in_data->priority_queues_filled) = 1;
    }

    pthread_barrier_wait(in_data->sync_barrier);

    if (in_data->workernumber == 0)
    {
        COUNT_PQ_PREPROCESSING_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    int local_pqs_stolen = 0;
    int current_pq_index;
    while (1)
    {
        current_pq_index = __sync_fetch_and_add(in_data->pq_counter, 1);

        if (current_pq_index >= *(in_data->final_pq_list_size))
        {
            break;
        }

        pqueue_t *pq = (*(in_data->final_pq_list))[current_pq_index];
        if (pq->is_stolen)
        {
            // printf("[WORKSTEALING MAIN NODE] - PQ %d where stolen by another node.\n", current_pq_index);
            local_pqs_stolen++;
            continue;
        }

        while (process_pq_of_batch_chatzakis(current_pq_index, in_data))
        {
            receive_shared_bsf_chatzakis(in_data->workernumber, in_data->bsf_result, in_data->shared_bsf_results, in_data->bsf_lock);
            share_bsf_chatzakis(in_data->bsf_result, in_data->workernumber);

            if (comm_data != NULL && in_data->workernumber == 0 && comm_data->mode == MODULE)
            {
                CALL_MODULE
            }
        }
    }

    pthread_barrier_wait(in_data->sync_barrier);
    if (in_data->workernumber == 0)
    {
        COUNT_QUEUE_PROCESS_TIME_END
    }

    pthread_mutex_lock(in_data->distances_lock);
    total_real_distance_counter += thread_real_distance_counter;
    total_min_distance_counter += thread_min_distance_counter;
    *(in_data->pqs_stolen) += local_pqs_stolen;
    pthread_mutex_unlock(in_data->distances_lock);

    pthread_exit(NULL);
}

batch_list *create_subtree_batches_simple_chatzakis(node_list *nodelist, int number_of_batches_to_create, int pq_th)
{
    batch_list *batchlist = malloc(sizeof(batch_list));

    batchlist->batch_amount = number_of_batches_to_create;
    batchlist->batches = malloc(sizeof(subtree_batch) * number_of_batches_to_create);

    int batch_size = nodelist->node_amount / batchlist->batch_amount;

    for (int i = 0; i < number_of_batches_to_create; i++)
    {
        memset(&batchlist->batches[i], 0, sizeof(subtree_batch)); // sets everything to 0. (mallon? :))

        batchlist->batches[i].id = i;
        batchlist->batches[i].from = i * batch_size;
        batchlist->batches[i].nodelist = nodelist;
        batchlist->batches[i].pq_th = pq_th;

        if (i == number_of_batches_to_create - 1)
        {
            batchlist->batches[i].to = nodelist->node_amount;                    //! padding-like
            batchlist->batches[i].size = nodelist->node_amount - i * batch_size; //! einai swsto auto?
        }
        else
        {
            batchlist->batches[i].to = (i + 1) * batch_size;
            batchlist->batches[i].size = batch_size;
        }

        pthread_mutex_init(&batchlist->batches[i].pq_insert_lock, NULL);

        batchlist->batches[i].max_pq_index = 0;
        batchlist->batches[i].min_pq_index = __INT32_MAX__;
    }

    return batchlist;
}

int estimate_th(double x, double (*estimation_func)(double))
{
    return (int)estimation_func(x);
}

extern int first_time_flag;
/**
 * @brief Odyssey exact search 
 * 
 * @param args Exact search arguments 
 * @return query_result Contains the float result of the query
 */
query_result exact_search_workstealing_chatzakis(search_function_params args)
{
    // static int first_time_flag = 1;

    int query_id = args.query_id;
    ts_type *ts = args.ts;
    ts_type *paa = args.paa;
    isax_index *index = args.index;
    node_list *nodelist = args.nodelist;
    float minimum_distance = args.minimum_distance;
    double (*estimation_func)(double) = args.estimation_func;
    communication_module_data *comm_data = args.comm_data;

    float *shared_bsf_results = args.shared_bsf_results;

    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);

    query_counter = query_id;

    SET_APPROXIMATE(bsf_result.distance);

    queries_statistics_manol[query_counter].initial_bsf = bsf_result.distance;

    if (bsf_sharing_pdr)
    {
        if (first_time_flag)
        {
            share_initial_bsf_chatzakis(bsf_result);
            first_time_flag = 0;
        }
        else
        {
            share_bsf_chatzakis_parent_func(&bsf_result);
        }
    }

    if (bsf_result.distance == 0)
    {
        return bsf_result;
    }

    int median = estimate_th(bsf_result.distance, estimation_func);
    int max_th = __INT32_MAX__;
    int query_th = median / th_div_factor;
    if (query_th == 0)
    {
        query_th = 1;
    }

    batch_list *batchlist = create_subtree_batches_simple_chatzakis(nodelist, maxquerythread, query_th);//!

    pthread_t threadid[maxquerythread];
    pthread_t workstealing_thread;
    pthread_t coordinator_thread_id;

    pthread_barrier_t sync_barrier;
    pthread_barrier_init(&sync_barrier, NULL, maxquerythread);

    pthread_mutex_t bsf_lock;
    pthread_mutex_init(&bsf_lock, NULL);

    pthread_mutex_t distances_lock;
    pthread_mutex_init(&distances_lock, NULL);

    volatile int batch_counter = 0;
    volatile int pq_counter = 0;

    volatile char query_workers_finished = 0;
    volatile char receiving_workstealing = 0;
    volatile char priority_queues_filled = 0;

    subtree_batch *batches = batchlist->batches;
    int total_batches = batchlist->batch_amount;

    pqueue_t **all_pqs = NULL;
    int all_pqs_size = 0;
    int stolen_pqs = 0;

    float bsf_recv_data[2] = {-1, -1};
    MPI_Request bsf_recv_requests[comm_sz];

    for (int rank = 0; rank < comm_sz; rank++)
    {
        MPI_Irecv(bsf_recv_data, 2, MPI_FLOAT, rank, WORKSTEALING_BSF_SHARE, MPI_COMM_WORLD, &bsf_recv_requests[rank]);
    }

    MESSI_workstealing_query_data_chatzakis workerdata[maxquerythread];

    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].workernumber = i;
        workerdata[i].ts = ts;
        workerdata[i].paa = paa;
        workerdata[i].minimum_distance = minimum_distance;

        workerdata[i].bsf_result = &bsf_result;

        workerdata[i].batches = batches;
        workerdata[i].total_batches = total_batches;
        workerdata[i].batch_counter = &batch_counter;

        workerdata[i].sync_barrier = &sync_barrier;
        workerdata[i].bsf_lock = &bsf_lock;

        workerdata[i].receiving_workstealing = &receiving_workstealing;

        workerdata[i].index = index;
        workerdata[i].pq_counter = &pq_counter;

        workerdata[i].final_pq_list = &all_pqs;
        workerdata[i].final_pq_list_size = &all_pqs_size;

        workerdata[i].distances_lock = &distances_lock;
        workerdata[i].priority_queues_filled = &priority_queues_filled;

        workerdata[i].pqs_stolen = &stolen_pqs;

        workerdata[i].bsf_received_data = bsf_recv_data;
        workerdata[i].bsf_receive_requests = bsf_recv_requests;

        //! new
        workerdata[i].comm_data = comm_data;
        workerdata[i].shared_bsf_results = shared_bsf_results;
    }

    if (bsf_sharing_pdr)
    {
        float candidate_bsf = try_to_receive_initial_bsfs_chatzakis(shared_bsf_results);

        if (shared_bsf_results[query_counter] < candidate_bsf)
        {
            if (ENABLE_PRINTS_BSF_SHARING)
            {
                printf("[BSF SHARING BOOK-KEEPING] Node %d starts with answer kept = %f for query %d, initial bsf was %f\n", my_rank, shared_bsf_results[query_counter], query_counter, bsf_result.distance);
            }
            candidate_bsf = shared_bsf_results[query_counter];
        }

        bsf_result.distance = candidate_bsf;
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_workstealing_thread_routine_chatzakis, (void *)&(workerdata[i]));
    }

    workstealing_thread_data ws_th_data;
    if (comm_sz > 1 && workstealing_chatzakis)
    {
        ws_th_data.query_workers_finished = &query_workers_finished;
        ws_th_data.receiving_workstealing = &receiving_workstealing;
        ws_th_data.priority_queues_filled = &priority_queues_filled;

        ws_th_data.bsf_lock = &bsf_lock;
        ws_th_data.bsf_result = &bsf_result;

        ws_th_data.batchlist = batchlist;

        pthread_create(&workstealing_thread, NULL, workstealing_thread_routine_chatzakis, (void *)&(ws_th_data));
    }

    if (comm_data != NULL && comm_data->mode == THREAD)
    {
        coordinator_data c_data;
        c_data.comm_data = comm_data;
        c_data.threads_finished = &query_workers_finished;
        pthread_create(&coordinator_thread_id, NULL, coordinator_thread_module_handling_chatzakis, (void *)&c_data);
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    query_workers_finished = 1;

    if (comm_sz > 1 && workstealing_chatzakis)
    {
        pthread_join(workstealing_thread, NULL);
    }

    if (comm_data != NULL && comm_data->mode == THREAD)
    {
        pthread_join(coordinator_thread_id, NULL);
    }

    pthread_barrier_destroy(&sync_barrier);
    pthread_mutex_destroy(&bsf_lock);

    for (int i = 0; i < all_pqs_size; i++)
    {
        free(all_pqs[i]);
        all_pqs[i] = NULL;
    }
    free(all_pqs);

    free(batchlist->batches);
    free(batchlist);

    bsf_result.stolen_pqs = stolen_pqs;
    bsf_result.total_pqs = all_pqs_size;

    return bsf_result;
}

/**
 * @brief Odyssey workstealing (helper node) algorithm
 * 
 * @param ws_args Workstealing algorithms
 * @return query_result Contains the float result of the query
 */
query_result exact_search_workstealing_helper_single_batch_chatzakis(ws_search_function_params ws_args)
{
    int query_id = ws_args.query_id;
    ts_type *ts = ws_args.ts;
    ts_type *paa = ws_args.paa;
    isax_index *index = ws_args.index;
    node_list *nodelist = ws_args.nodelist;
    float minimum_distance = ws_args.minimum_distance;
    double (*estimation_func)(double) = ws_args.estimation_func;
    float bsf = ws_args.bsf;
    int *batches_to_create = ws_args.batch_ids;
    float *shared_bsf_results = ws_args.shared_bsf_results;

    query_result bsf_result;
    bsf_result.distance = bsf;

    query_counter = query_id;

    if (bsf_sharing_pdr)
    {
        if (first_time_flag)
        {
            share_initial_bsf_chatzakis(bsf_result);
            first_time_flag = 0;
        }
        else
        {
            share_bsf_chatzakis_parent_func(&bsf_result);
        }
    }

    if (bsf_result.distance == 0)
    {
        return bsf_result;
    }

    int median = estimate_th(bsf_result.distance, estimation_func);
    int query_th = median / th_div_factor;
    if (query_th == 0)
    {
        query_th = 1;
    }

    batch_list *batchlist = create_subtree_batches_simple_chatzakis(nodelist, maxquerythread, query_th);

    pthread_t threadid[maxquerythread];

    pthread_barrier_t sync_barrier;
    pthread_barrier_init(&sync_barrier, NULL, maxquerythread);

    pthread_mutex_t bsf_lock;
    pthread_mutex_init(&bsf_lock, NULL);

    pthread_mutex_t distances_lock;
    pthread_mutex_init(&distances_lock, NULL);

    volatile int batch_counter = 0;
    volatile int pq_counter = 0;

    volatile char query_workers_finished = 0;
    volatile char receiving_workstealing = 1;
    volatile char priority_queues_filled = 0;

    subtree_batch batches[batches_to_send]; //&((batchlist->batches)[batch_id]);
    int total_batches = batches_to_send;    // batchlist->batch_amount;
    for (int i = 0; i < batches_to_send; i++)
    {
        int batch_id = batches_to_create[i];
        batches[i] = (batchlist->batches)[batch_id];
    }

    pqueue_t **all_pqs = NULL;
    int all_pqs_size = 0;

    int pqs_stolen = 0;

    float bsf_recv_data[2] = {-1, -1};
    MPI_Request bsf_recv_requests[comm_sz];

    // int coordinator_of_current_group_rank = FIND_COORDINATOR_NODE_RANK(my_rank, node_group_total_nodes);
    // for (int rank = coordinator_of_current_group_rank; rank < (coordinator_of_current_group_rank + node_group_total_nodes); rank++)
    //{
    //     MPI_Irecv(bsf_recv_data, 2, MPI_FLOAT, rank, WORKSTEALING_BSF_SHARE, MPI_COMM_WORLD, &bsf_recv_requests[rank]);
    // }

    MESSI_workstealing_query_data_chatzakis workerdata[maxquerythread];

    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].workernumber = i;
        workerdata[i].ts = ts;
        workerdata[i].paa = paa;
        workerdata[i].minimum_distance = minimum_distance;

        workerdata[i].bsf_result = &bsf_result;

        workerdata[i].batches = batches;
        workerdata[i].total_batches = total_batches;
        workerdata[i].batch_counter = &batch_counter;

        workerdata[i].sync_barrier = &sync_barrier;
        workerdata[i].bsf_lock = &bsf_lock;

        workerdata[i].receiving_workstealing = &receiving_workstealing;

        workerdata[i].index = index;
        workerdata[i].pq_counter = &pq_counter;

        workerdata[i].final_pq_list = &all_pqs;
        workerdata[i].final_pq_list_size = &all_pqs_size;

        workerdata[i].distances_lock = &distances_lock;
        workerdata[i].priority_queues_filled = &priority_queues_filled;

        workerdata[i].pqs_stolen = &pqs_stolen;

        // new
        // workerdata[i].bsf_received_data = bsf_recv_data;
        // workerdata[i].bsf_receive_requests = bsf_recv_requests;

        workerdata[i].shared_bsf_results = shared_bsf_results;
    }

    if (bsf_sharing_pdr)
    {
        float candidate_bsf = try_to_receive_initial_bsfs_chatzakis(shared_bsf_results);

        if (shared_bsf_results[query_counter] < candidate_bsf)
        {
            if (ENABLE_PRINTS_BSF_SHARING)
            {
                printf("[BSF SHARING BOOK-KEEPING] Node %d starts with answer kept = %f for query %d, initial bsf was %f\n", my_rank, shared_bsf_results[query_counter], query_counter, bsf_result.distance);
            }
            candidate_bsf = shared_bsf_results[query_counter];
        }

        bsf_result.distance = candidate_bsf;
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, exact_search_workstealing_thread_routine_chatzakis, (void *)&(workerdata[i])); // workstealing_thread_routine_chatzakis
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    query_workers_finished = 1;

    pthread_barrier_destroy(&sync_barrier);
    pthread_mutex_destroy(&bsf_lock);

    for (int i = 0; i < all_pqs_size; i++)
    {
        free(all_pqs[i]);
        all_pqs[i] = NULL;
    }
    free(all_pqs);

    free(batchlist->batches);
    free(batchlist);

    bsf_result.stolen_pqs = pqs_stolen;
    bsf_result.total_pqs = all_pqs_size;
    return bsf_result;
}

// UTILS CHATZAKIS
void merge_pq_arr(int N, pqueue_t *arr[N], int l, int m, int r, int (*pq_cmp)(pqueue_t *p1, pqueue_t *p2))
{
    int i, j, k;
    int n1 = m - l + 1;
    int n2 = r - m;

    pqueue_t *L[n1], *R[n2];

    // Copy data to temp arrays L[] and R[]
    for (i = 0; i < n1; i++)
        L[i] = arr[l + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[m + 1 + j];

    // Merge the temp arrays back into arr[l..r]
    i = 0; // Initial index of first subarray
    j = 0; // Initial index of second subarray
    k = l; // Initial index of merged subarray
    while (i < n1 && j < n2)
    {
        if (/*(L[i].priority > R[j].priority*/ pq_cmp(L[i], R[j]))
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

    // Copy the remaining elements of L[], if there are any
    while (i < n1)
    {
        arr[k] = L[i];
        i++;
        k++;
    }

    // Copy the remaining elements of R[], if there are any
    while (j < n2)
    {
        arr[k] = R[j];
        j++;
        k++;
    }
}

void mergesort_pqs(int N, pqueue_t *pqs[N], int l, int r, int (*pq_cmp)(pqueue_t *p1, pqueue_t *p2))
{
    if (l < r)
    {
        int m = l + (r - l) / 2;

        mergesort_pqs(N, pqs, l, m, pq_cmp);
        mergesort_pqs(N, pqs, m + 1, r, pq_cmp);

        merge_pq_arr(N, pqs, l, m, r, pq_cmp);
    }
}

void sort_pqs_chatzakis(int N, pqueue_t *pqs[N], int (*pq_cmp)(pqueue_t *p1, pqueue_t *p2))
{
    mergesort_pqs(N, pqs, 0, N - 1, pq_cmp);
}

void shuffle_pq_array(size_t n, pqueue_t *array[n])
{
    if (n > 1)
    {
        size_t i;
        for (i = 0; i < n - 1; i++)
        {
            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            pqueue_t *t = array[j];
            array[j] = array[i];
            array[i] = t;
        }
    }
}