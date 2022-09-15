#ifndef parallel_parallel_index_engine_h
#define parallel_parallel_index_engine_h
#include "../../config.h"
#include "../../globals.h"
#include "sax/ts.h"
#include "sax/sax.h"	
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include "isax_index.h"
#include "isax_query_engine.h"

isax_node * insert_to_pRecBuf(parallel_first_buffer_layer *fbl,
									sax_type *sax,
									file_position_type *pos,
									root_mask_type mask, 
									isax_index *index, 
									pthread_mutex_t *lock_firstnode,
									int workernumber,
									int total_workernumber);
isax_node * insert_to_pRecBuf_ekosmas(parallel_first_buffer_layer_ekosmas *fbl,
									sax_type *sax,
									file_position_type *pos,
									root_mask_type mask, 
									isax_index *index, 
									pthread_mutex_t *lock_firstnode,
									int workernumber,
									int total_workernumber);
isax_node * insert_to_2pRecBuf(parallel_dfirst_buffer_layer *fbl,
									sax_type *sax,
									file_position_type *pos,
									root_mask_type mask, 
									isax_index *index, 
									pthread_mutex_t *lock_firstnode,
									int workernumber,
									int total_workernumber);
isax_node * insert_to_pRecBuf_lock_free(parallel_first_buffer_layer_ekosmas_lf *fbl, 
									sax_type *sax,
                          			file_position_type *pos,
                          			root_mask_type mask,
                          			isax_index *index, 
                          			int workernumber,
                          			int total_workernumber,
                          			const char parallelism_in_subtree);

typedef struct index_buffer_data
{
	//int ts_num,ts_loaded;
	file_position_type pos;
	isax_index *index;
	ts_type * ts;
	sax_type *saxv;
	int fin_number,blocid;
	pthread_mutex_t *lock_record;
	pthread_mutex_t *lock_fbl;
	pthread_mutex_t *lock_index;
	pthread_mutex_t *lock_cbl;
	pthread_mutex_t *lock_firstnode;
	pthread_mutex_t *lock_nodeconter;
	pthread_mutex_t *lock_disk;
	file_position_type *fbl;
	int *bufferpresize;
	int workernumber;
	int total_workernumber;
	int *nodecounter;
	pthread_barrier_t *lock_barrier1, *lock_barrier2, *lock_barrier3;
	bool finished;
}index_buffer_data;


// int read_block_length;	
unsigned long read_block_length;			// CHANGED BY EKOSMAS, JUNE 10, 2020
unsigned long ts_group_length;				// ADDED BY EKOSMAS, JUNE 10, 2020
// unsigned long backoff_time;					// ADDED BY EKOSMAS, JUNE 12, 2020
unsigned long backoff_multiplier;			// ADDED BY EKOSMAS, JUNE 24, 2020
#endif