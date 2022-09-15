//
//  first_buffer_layer.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/20/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isaxlib_first_buffer_layer_h
#define isaxlib_first_buffer_layer_h
#include "../../config.h"
#include "../../globals.h"
#include "isax_node.h"
#include "isax_index.h"

typedef struct fbl_soft_buffer {
    isax_node *node;
    sax_type ** sax_records;
    file_position_type ** pos_records;
    int initialized; 
    int max_buffer_size;
    int buffer_size;
} fbl_soft_buffer;

// EKOSMAS: STRUCT READ
typedef struct parallel_fbl_soft_buffer {
    isax_node* volatile node;                       // EKOSMAS: ADDED 'volatile' JUNE 16 2020 - CHANGED JUNE 24 2020
    sax_type ** sax_records;
    file_position_type ** pos_records;
    volatile int initialized;                       // EKOSMAS: ADDED 'volatile'
    int *max_buffer_size;                           // EKOSMAS: Why is this a pointer?
    int *buffer_size;                               // EKOSMAS: Why is this a pointer?
    volatile int finished;                          // EKOSMAS: ADDED 'volatile'
} parallel_fbl_soft_buffer;

typedef struct parallel_fbl_soft_buffer_ekosmas {
    isax_node* volatile node;                       // EKOSMAS: ADDED 'volatile' JUNE 16 2020 - CHANGED JUNE 24 2020
    sax_type **sax_records;
    file_position_type **pos_records;
    volatile int initialized;                       // EKOSMAS: ADDED 'volatile'
    int *max_buffer_size;                           // EKOSMAS: Why is this a pointer?
    int *buffer_size;                               // EKOSMAS: Why is this a pointer?
    volatile int finished;                          // EKOSMAS: ADDED 'volatile'
} parallel_fbl_soft_buffer_ekosmas;


// typedef struct {
//     isax_node* volatile node;
//     isax_node_record *record;
//     unsigned long buf_pos;
// } announce_rec;

typedef struct parallel_fbl_soft_buffer_ekosmas_lf {
    isax_node* volatile node;                       // EKOSMAS: ADDED 'volatile' JUNE 16 2020 - CHANGED JUNE 24 2020
    sax_type **sax_records;
    file_position_type **pos_records;
    volatile unsigned char initialized;             // EKOSMAS: ADDED 'volatile'
    int *max_buffer_size;                           // EKOSMAS: Why is this a pointer?
    int *buffer_size;                               // EKOSMAS: Why is this a pointer?
    volatile unsigned char processed;               // EKOSMAS: CHANGED from finished to processed
    volatile unsigned long next_iSAX_group;         // EKOSMAS: ADDED JULY 07, 2020
    root_mask_type mask;                            // EKOSMAS: ADDED JUNE 16, 2020
    volatile unsigned char **iSAX_processed;        // EKOSMAS: ADDED JULY 20, 2020
    // volatile announce_rec *announce_array;          // EKOSMAS: ADDED JULY 29, 2020
    volatile unsigned char recBuf_helpers_exist;    // EKOSMAS: ADDED AUGUST 07, 2020
} parallel_fbl_soft_buffer_ekosmas_lf;

typedef struct parallel_dfbl_soft_buffer {
    isax_node *node;
    sax_type *** sax_records;
    file_position_type *** pos_records;
    int initialized; 
    int *max_buffer_size;
    int *buffer_size;
    int finished;
} parallel_dfbl_soft_buffer;

typedef struct first_buffer_layer {
    int number_of_buffers;
    int initial_buffer_size;
    int max_total_size;
    int current_record_index;
    fbl_soft_buffer *soft_buffers;
    char *current_record;
    char *hard_buffer;
} first_buffer_layer;

typedef struct parallel_first_buffer_layer {
    int number_of_buffers;
    int initial_buffer_size;
    int max_total_size;
    int current_record_index;
    parallel_fbl_soft_buffer *soft_buffers;
    char *current_record;
    char *hard_buffer;
    int total_worker_number;
} parallel_first_buffer_layer;

typedef struct parallel_first_buffer_layer_ekosmas {
    int number_of_buffers;
    int initial_buffer_size;
    int max_total_size;
    int current_record_index;
    parallel_fbl_soft_buffer_ekosmas *soft_buffers;
} parallel_first_buffer_layer_ekosmas;

typedef struct parallel_first_buffer_layer_ekosmas_lf {
    int number_of_buffers;
    int initial_buffer_size;
    int max_total_size;
    int current_record_index;
    parallel_fbl_soft_buffer_ekosmas_lf *soft_buffers;
} parallel_first_buffer_layer_ekosmas_lf;

typedef struct parallel_dfirst_buffer_layer {
    int number_of_buffers;
    int initial_buffer_size;
    int max_total_size;
    int current_record_index;
    char *current_record;
    char *hard_buffer;
    parallel_dfbl_soft_buffer *soft_buffers;
    int total_worker_number;
} parallel_dfirst_buffer_layer;

typedef struct isax_index isax_index;

typedef struct trans_fbl_input
{
    int start_number,stop_number,conternumber;
    isax_index *index;
    pthread_mutex_t *lock_index;
    pthread_mutex_t *lock_fbl_conter;
    pthread_mutex_t *lock_write;
    first_buffer_layer *fbl;
    int preworkernumber;
    int fbloffset;
    int *buffersize;
    pthread_barrier_t *lock_barrier1, *lock_barrier2, *lock_barrier3;
    bool finished;
}trans_fbl_input;

first_buffer_layer * initialize_fbl(int initial_buffer_size, int max_fbl_size, 
                                    int max_total_size, isax_index *index);

parallel_first_buffer_layer * initialize_pRecBuf(int initial_buffer_size, int max_fbl_size, 
                                    int max_total_size, isax_index *index);
parallel_first_buffer_layer_ekosmas * initialize_pRecBuf_ekosmas(int initial_buffer_size, int max_fbl_size, 
                                    int max_total_size, isax_index *index);
parallel_first_buffer_layer_ekosmas_lf * initialize_pRecBuf_ekosmas_lf(int initial_buffer_size, int max_fbl_size, 
                                    int max_total_size, isax_index *index);

void destroy_fbl(first_buffer_layer *fbl);
void destroy_pRecBuf(parallel_first_buffer_layer *fbl,int prewokernumber);
isax_node * insert_to_fbl(first_buffer_layer *fbl, sax_type *sax,
                          file_position_type * pos, root_mask_type mask, 
                          isax_index *index);

enum response flush_fbl(first_buffer_layer *fbl, isax_index *index);

#endif
