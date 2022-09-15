
#ifndef al_parallel_inmemory_query_engine_h
#define al_parallel_inmemory_query_engine_h
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
#include "parallel_query_engine.h"
#include "isax_node.h"
#include "pqueue.h"
#include "isax_first_buffer_layer.h"
#include "ads/isax_node_split.h"
#include "inmemory_index_engine.h"

typedef struct localStack
{
	isax_node **val;
	int top;
	int bottom;
} localStack;

typedef struct MESSI_workerdata
{
	isax_node *current_root_node;
	ts_type *paa, *ts;
	pqueue_t *pq;
	isax_index *index;
	float minimum_distance;
	int limit;
	pthread_mutex_t *lock_current_root_node;
	pthread_mutex_t *lock_queue;
	pthread_barrier_t *lock_barrier;
	pthread_rwlock_t *lock_bsf;
	query_result *bsf_result;
	volatile int *node_counter; // EKOSMAS, AUGUST 29 2020: Added volatile
	isax_node **nodelist;
	int amountnode;
	localStack *localstk;
	localStack *allstk;
	pthread_mutex_t *locallock, *alllock;
	int *queuelabel, *allqueuelabel;
	pqueue_t **allpq;
	int startqueuenumber;
	pqueue_bsf *pq_bsf;
	int workernumber; // EKOSMAS, AUGUST 29 2020: Added
} MESSI_workerdata;

typedef struct MESSI_workerdata_ekosmas
{
	// isax_node *current_root_node;			// EKOSMAS, 29 AUGUST 2020: REMOVED
	ts_type *paa, *ts;
	// pqueue_t *pq;							// EKOSMAS, 29 AUGUST 2020: REMOVED
	isax_index *index;
	float minimum_distance;
	// int limit;								// EKOSMAS, 29 AUGUST 2020: REMOVED
	// pthread_mutex_t *lock_current_root_node;	// EKOSMAS, 30 AUGUST 2020: REMOVED
	// pthread_mutex_t *lock_queue;				// EKOSMAS, 29 AUGUST 2020: REMOVED
	pthread_barrier_t *lock_barrier;
	pthread_mutex_t *lock_bsf;
	query_result *bsf_result;
	volatile int *node_counter; // EKOSMAS, AUGUST 29 2020: Added volatile
	isax_node **nodelist;
	int amountnode;
	// localStack *localstk; 					// EKOSMAS, 29 AUGUST 2020: REMOVED
	// localStack *allstk;						// EKOSMAS, 29 AUGUST 2020: REMOVED
	// pthread_mutex_t *locallock;				// EKOSMAS, 29 AUGUST 2020: REMOVED
	pthread_mutex_t *alllock;
	// int *queuelabel;							// EKOSMAS, 29 AUGUST 2020: REMOVED
	int *allqueuelabel;
	pqueue_t **allpq;
	int startqueuenumber;
	// pqueue_bsf *pq_bsf;						// EKOSMAS, 29 AUGUST 2020: REMOVED
	int workernumber; // EKOSMAS, AUGUST 29 2020: Added

	int workstealing_round; // manol: 07.05.2021 Added
	int workstealing_on_which_tree_i_work;

	// Chatzakis patch
	pthread_mutex_t *distances_lock_per_thread;
	int num_prqueues;
	unsigned long long int *total_real_distance_counter;
	unsigned long long int *total_min_distance_counter;
	int queryID;
	communication_module_data *comm_data;
	volatile int *threads_finished;
	pthread_mutex_t *threads_finished_mutex;

	float *shared_bsf_results;

} MESSI_workerdata_ekosmas;

typedef struct MESSI_workstealing_query_data_chatzakis
{
	int workernumber;
	float minimum_distance;

	ts_type *paa, *ts;
	isax_index *index;

	query_result *bsf_result;

	subtree_batch *batches;
	int total_batches;

	volatile int *batch_counter;
	volatile int *pq_counter;

	pthread_barrier_t *sync_barrier;
	pthread_mutex_t *bsf_lock;

	volatile char *receiving_workstealing;
	volatile char *priority_queues_filled;

	pqueue_t ***final_pq_list;
	int *final_pq_list_size;

	pthread_mutex_t *distances_lock;

	int *pqs_stolen;

	MPI_Request *bsf_receive_requests;
	float *bsf_received_data;

	communication_module_data *comm_data;

	float *shared_bsf_results;

} MESSI_workstealing_query_data_chatzakis;

typedef struct MESSI_workerdata_chatzakis
{
	ts_type *paa, *ts;
	isax_index *index;
	float minimum_distance;
	pthread_barrier_t *lock_barrier;
	pthread_mutex_t *lock_bsf;
	query_result *bsf_result;
	volatile int *node_counter;
	isax_node **nodelist;
	int amountnode;

	pthread_mutex_t *alllock;
	int *allqueuelabel;
	pqueue_t **allpq;
	int startqueuenumber;
	int workernumber;

	node_group *nodegroups;
	int node_groups_size;

} MESSI_workerdata_chatzakis;

typedef struct MESSI_workerdata_ekosmas_EP
{
	ts_type *paa, *ts;
	pqueue_t *pq;
	isax_index *index;
	float minimum_distance;
	pthread_mutex_t *lock_bsf;
	query_result *bsf_result;
	volatile int *node_counter;
	isax_node **nodelist;
	int amountnode;
	int workernumber;
} MESSI_workerdata_ekosmas_EP;

typedef struct MESSI_workerdata_ekosmas_lf
{
	ts_type *paa, *ts;
	pqueue_t **allpq;
	query_result ***allpq_data;
	volatile unsigned char *queue_finished;
	volatile unsigned char *helper_queue_exist;
	volatile int *fai_queue_counters;
	volatile float **queue_bsf_distance;
	isax_index *index;
	float minimum_distance;
	query_result *volatile *bsf_result_p;
	volatile int *node_counter;
	volatile int *sorted_array_counter;
	isax_node **nodelist;
	int amountnode;
	int workernumber;
	char parallelism_in_subtree;
	volatile unsigned long *next_queue_data_pos;
	int query_id;
} MESSI_workerdata_ekosmas_lf;

float calculate_node_distance_inmemory_m(isax_index *index, isax_node *node, ts_type *query, float bsf);

query_result approximate_search_inmemory_m(ts_type *ts, ts_type *paa, isax_index *index);
query_result refine_answer_inmemory_m(ts_type *ts, ts_type *paa, isax_index *index, query_result approximate_bsf_result, float minimum_distance, int limit);

query_result exact_search_ParISnew_inmemory_hybrid(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
												   float minimum_distance, int min_checked_leaves);
void *exact_search_worker_inmemory_hybridpqueue(void *rfdata);
void insert_tree_node_m_hybridpqueue(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber);

query_result exact_search_ParISnew_inmemory_hybrid_ekosmas(search_function_params args);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas(void *rfdata);
void insert_tree_node_m_hybridpqueue_ekosmas(float *paa, isax_node *node, isax_index *index, query_result *bsf_result /* float bsf <-- ekosmas*/, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber, int workernumber);
void insert_tree_node_m_hybridpqueue_ekosmas_tree_stats(float *paa, isax_node *node, isax_index *index, query_result *bsf_result /* float bsf <-- ekosmas*/, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber, int workernumber, int node_num);

query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_EP(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
															  float minimum_distance);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_EP(void *rfdata);
void insert_tree_node_m_hybridpqueue_ekosmas_EP(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t *my_pq);

query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_lf(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
															  float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id);

void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_not_mixed(void *rfdata);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_only_after_helper(void *rfdata);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_full_helping(void *rfdata);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_no_helping(void *rfdata);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_queue_full_help(void *rfdata);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_sorted_array(void *rfdata);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_only_after_helper(void *rfdata);
void *exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping(void *rfdata);

void add_to_queue_data_lf(float *paa, isax_node *node, isax_index *index, float bsf, query_result ***pq_data, int *tnumber, volatile unsigned long *next_queue_data_pos, const int query_id);

// declare into ads.c
extern float *tmp_raw_file; // 19-04-2021 renamed (old name: rawfile) // this is used only in query answering time!!! (when we compute real distances we want to point to the raw dataseries)
void pushbottom(localStack *stk, isax_node *node);
isax_node *poptop(localStack *stk);
isax_node *popbottom(localStack *stk);
bool isemptyqueue(localStack *stk);
isax_node *poptop2(localStack *stk);
isax_node *popbottom2(localStack *stk);

// CHATZAKIS
batch_list *create_subtree_batches_simple_chatzakis(node_list *nodelist, int number_of_batches_to_create, int pq_th);

query_result exact_search_ParISnew_inmemory_hybrid_parallel_chatzakis(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist, float minimum_distance, int queryNum, unsigned long long int *min, unsigned long long int *real);
void *exact_search_worker_inmemory_hybridpqueue_parallel_chatzakis(void *rfdata);
query_result exact_search_ParISnew_inmemory_hybrid_no_approximate_calc_chatzakis(dress_query *d_query, ts_type *paa, isax_index *index, node_list *nodelist, float minimum_distance, communication_module_data *comm_data);
query_result exact_search_ParISnew_inmemory_hybrid_dynamic_module_chatzakis(search_function_params args);

void *exact_search_workstealing_thread_routine_chatzakis(void *rfdata);
query_result exact_search_workstealing_chatzakis(search_function_params args);
query_result exact_search_workstealing_helper_single_batch_chatzakis(ws_search_function_params ws_args);

// UTILS
void shuffle_pq_array(size_t n, pqueue_t *pqarr[n]);
void sort_pqs_chatzakis(int N, pqueue_t *pqs[N], int (*pq_cmp)(pqueue_t *p1, pqueue_t *p2));

#endif
