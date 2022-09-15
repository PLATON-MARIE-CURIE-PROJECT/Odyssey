//
//  Updated by Eleftherios Kosmas on May 2020.
//

#ifndef al_inmemory_index_engine_h
#define al_inmemory_index_engine_h

#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <pthread.h>
#include "isax_index.h"
#include "isax_query_engine.h"
#include "parallel_query_engine.h"
#include "isax_node.h"
#include "pqueue.h"
#include "isax_first_buffer_layer.h"
#include "ads/isax_node_split.h"

void index_creation_pRecBuf_new(const char *ifilename, long int ts_num, isax_index *index);
float *index_creation_pRecBuf_new_ekosmas(const char *ifilename, long int ts_num, isax_index *index, int create_index_flag);
float *index_creation_pRecBuf_new_ekosmas_MESSI_with_enhanced_blocking_parallelism(const char *ifilename, long int ts_num, isax_index *index, int create_index_flag);
void *index_creation_pRecBuf_worker_new(void *transferdata);
void *index_creation_pRecBuf_worker_new_ekosmas(void *transferdata);
root_mask_type isax_pRecBuf_index_insert_inmemory(isax_index *index,
												  sax_type *sax,
												  file_position_type *pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber);
root_mask_type isax_pRecBuf_index_insert_inmemory_ekosmas(isax_index *index,
														  sax_type *sax,
														  file_position_type *pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber);

enum response flush_subtree_leaf_buffers_inmemory(isax_index *index, isax_node *node);

typedef struct // MESSI receive buffer for process worker number
{
	isax_index *index;
	int start_number, stop_number;
	ts_type *ts;
	pthread_mutex_t *lock_record;
	pthread_mutex_t *lock_fbl;
	pthread_mutex_t *lock_index;
	pthread_mutex_t *lock_cbl;
	pthread_mutex_t *lock_firstnode; //
	pthread_mutex_t *lock_nodeconter;
	pthread_mutex_t *lock_disk;
	int workernumber;		// worker processing this buffer
	int total_workernumber; //
	pthread_barrier_t *lock_barrier1, *lock_barrier2;
	int *node_counter;
	// bool finished;
	int *nodeid;
	unsigned long *shared_start_number;
	int myid;
} buffer_data_inmemory;

typedef struct
{
	isax_index *index;
	int ts_num;
	int workernumber;
	pthread_mutex_t *lock_firstnode;
	pthread_barrier_t *wait_summaries_to_compute;
	int *node_counter;
	// bool finished;
	unsigned long *shared_start_number;
	char parallelism_in_subtree;
	volatile unsigned long *next_iSAX_group;

	float *rawfile;
} buffer_data_inmemory_ekosmas;

typedef struct transferfblinmemory
{
	int start_number, stop_number, conternumber;
	int preworkernumber;
	isax_index *index;
	int *nodeid;
} transferfblinmemory;

// float *rawfile; // 19-04-2021

// ----------------------------------------------
// ekosmas:
// ----------------------------------------------
volatile unsigned char *block_processed;
volatile unsigned char *ts_processed;

typedef struct next_ts_grp
{
	volatile unsigned long num CACHE_ALIGN;
	char pad[PAD_CACHE(sizeof(unsigned long))];
} next_ts_group;

next_ts_group *next_ts_group_read_in_block;
volatile unsigned char all_blocks_processed;
volatile unsigned char all_RecBufs_processed;
volatile unsigned char *block_helper_exist;
volatile unsigned char *block_helpers_num;
volatile unsigned char *recBuf_helpers_num;

static __thread double my_time_for_blocks_processed = 0;
static __thread unsigned long my_num_blocks_processed = 0;
static __thread double my_time_for_subtree_construction = 0;
static __thread unsigned long my_num_subtree_construction = 0;
static __thread unsigned long my_num_subtree_nodes = 0;
static __thread struct timeval my_time_start_val;
static __thread struct timeval my_current_time_val;
static __thread double my_tS;
static __thread double my_tE;

#define COUNT_MY_TIME_START gettimeofday(&my_time_start_val, NULL);
#define COUNT_MY_TIME_FOR_BLOCKS_END                                              \
	gettimeofday(&my_current_time_val, NULL);                                     \
	my_tS = my_time_start_val.tv_sec * 1000000 + (my_time_start_val.tv_usec);     \
	my_tE = my_current_time_val.tv_sec * 1000000 + (my_current_time_val.tv_usec); \
	my_time_for_blocks_processed += (my_tE - my_tS);
#define COUNT_MY_TIME_FOR_SUBTREE_END                                             \
	gettimeofday(&my_current_time_val, NULL);                                     \
	my_tS = my_time_start_val.tv_sec * 1000000 + (my_time_start_val.tv_usec);     \
	my_tE = my_current_time_val.tv_sec * 1000000 + (my_current_time_val.tv_usec); \
	my_time_for_subtree_construction += (my_tE - my_tS);
#define BACKOFF_BLOCK_DELAY_VALUE (my_time_for_blocks_processed / my_num_blocks_processed)
#define BACKOFF_SUBTREE_DELAY_PER_NODE (my_time_for_subtree_construction / my_num_subtree_nodes)

static __thread unsigned long blocks_helped_cnt = 0;
static __thread unsigned long blocks_helping_avoided_cnt = 0;
static __thread unsigned long recBufs_helped_cnt = 0;
static __thread unsigned long recBufs_helping_avoided_cnt = 0;

// ----------------------------------------------

typedef struct node_list
{
	isax_node **nlist;
	int node_amount;

	int data_amount;
	ts_type *rawfile;
} node_list;

typedef struct node_list_lf
{
	parallel_fbl_soft_buffer_ekosmas_lf **nlist;
	int node_amount;
} node_list_lf;

// Chatzakis
long int find_total_nodes(isax_node *root_node);
long int find_total_leafs_nodes(isax_node *root_node);
long int find_tree_height(isax_node *root_node);
long int find_total_tree_leafs_depths(isax_node *root_node, long int depth);
long int count_ts_in_nodes(isax_node *root_node, const char parallelism_in_subtree, const char recBuf_helpers_exist);
void get_tree_statistics_manol(isax_index *index);
void get_tree_statistics_chatz(isax_index *index);

float *index_creation_node_groups_chatzakis(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree, pdr_node_group *node_groups, int total_node_groups, int total_nodes_per_nodegroup);

// todel
typedef struct node_group
{
	int group_id;

	node_list *nodelist;
	int from;
	int to;
	int size;
	volatile int current_subtree_index;

	char processed;

	pqueue_t *pq;
	pthread_mutex_t pqlock;
} node_group;

typedef struct subtree_batch
{
	int id;
	int from;
	int to;
	int size;

	volatile int current_subtree_to_process;
	volatile int current_pq_to_process;

	char processed_phase_1;
	char processed_phase_2;

	char is_getting_help_phase1;
	char is_getting_help_phase2;

	int pq_th;
	int pq_amount;

	pthread_mutex_t pq_insert_lock;
	pqueue_t *pq[MAX_PQs_WORKSTEALING];

	node_list *nodelist;

	int max_pq_index;
	int min_pq_index;

	int is_stolen;
} subtree_batch;

typedef struct batch_list
{
	subtree_batch *batches;
	int batch_amount;
} batch_list;

// May 2022: Refactoring for partial data replication

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


/*Botao to Manos: start copy*/
static unsigned int GraytoDecimal(unsigned int x);
static unsigned int DecimaltoGray(unsigned int x);
void countmemory(parallel_first_buffer_layer *current_fbl,int *counterofmemory);
void distribute_node_data(parallel_fbl_soft_buffer *current_buffer,int come_sz);
void distribute_node_data_2(parallel_fbl_soft_buffer *current_buffer,int come_sz);

void distribute_max_node(parallel_first_buffer_layer *current_fbl,int nodenumber);
void distribute_max_node_uni(parallel_first_buffer_layer *current_fbl,int roundnumber);

void datadistribute(parallel_first_buffer_layer *current_fbl,int *counterofmemory);
void data_distribute_input(parallel_first_buffer_layer *current_fbl,int *counterofmemory,int round,float brate);
void data_distribute_input_2(parallel_first_buffer_layer *current_fbl,int *counterofmemory,int round,float brate);

file_position_type **collect_position_buffer(parallel_first_buffer_layer *current_fbl,int *counterofmemory);
node_list * index_creation_mpi_botao_uni_input_fast(const char *ifilename, long int ts_num, isax_index *index, int myrank,int round,float brate);
node_list * preprocessing_botao_to_manos(const char *ifilename, long int ts_num, isax_index *index, int myrank,int group_node_number,int round,float brate);

//ifilename：the path of the dataset file
//ts_num: the dataset size
//index: the index
//myrank: the id of the data part distributed
//group_node_number: total node in 1 group
//round: the number of buffer distributed 
//brate: balance rate
//example: nodelist=preprocessing_botao_to_manos(dataset, dataset_size, idx_1,0,4,8000,1.05f);
//nodelist.rawfile is the raw data array need be index for the current node.
//nodelist.data_amount is the data size of the rawfile array.
/*Botao to Manos: end copy*/

float *index_creation_node_groups_chatzakis_botao_prepro(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree, pdr_node_group *node_groups, int total_node_groups, int total_nodes_per_nodegroup);


#endif
