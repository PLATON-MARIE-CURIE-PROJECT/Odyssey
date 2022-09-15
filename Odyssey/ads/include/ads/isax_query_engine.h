//
//  isax_query_engine.h
//  al_isax
//
//  Created by Kostas Zoumpatianos on 4/13/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef al_isax_isax_query_engine_h
#define al_isax_isax_query_engine_h
#include "../../config.h"
#include "../../globals.h"
#include "isax_index.h"
#include "isax_node.h"
#include "pqueue.h"

#include <mpi.h>

// gonna move
typedef struct query_result
{
	volatile float distance; // EKOSMAS 15 SEPTEMBER 2020: During lock-free versions, this changes to negative (-1) to inform that this queue node has been processed
	
	isax_node *node;
	size_t pqueue_position;
	float init_bsf;

	/*Chatzakis*/
	int total_pqs;
	int stolen_pqs;

} query_result;

static int
cmp_pri(double next, double curr)
{
	return (next > curr);
}

static double
get_pri(void *a)
{
	return (double)((query_result *)a)->distance;
}

static void
set_pri(void *a, double pri)
{
	((query_result *)a)->distance = (float)pri;
}

static size_t
get_pos(void *a)
{
	return ((query_result *)a)->pqueue_position;
}

static void
set_pos(void *a, size_t pos)
{
	((query_result *)a)->pqueue_position = pos;
}

// CHATZAKIS
typedef struct dress_query
{
	int id;

	double initial_estimation;

	ts_type *query;

	query_statistics_t stats;
	priority_q priority;
	query_result initialBSF;

	int avg_pq_size_estimation;

} dress_query;

typedef struct communication_module_data
{
	int q_num;
	int *q_loaded; // current query loaded!
	int *process_buffer;
	int *rec_message;
	int *termination_message_id;

	dynamic_modes mode;

	MPI_Request *request;
	MPI_Request *send_request;

	int (*module_func)(int *q_loaded, int q_num, int *process_buffer, MPI_Request *request, int *rec_message, MPI_Request *send_request, int *termination_message_id);

} communication_module_data;

#endif
