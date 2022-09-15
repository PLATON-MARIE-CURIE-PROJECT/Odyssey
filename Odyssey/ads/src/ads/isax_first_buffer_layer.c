//
//  first_buffer_layer.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/20/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "ads/sax/sax.h"
#include "ads/isax_first_buffer_layer.h"

struct first_buffer_layer * initialize_fbl(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index)
{
    struct first_buffer_layer *fbl = malloc(sizeof(first_buffer_layer));

    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;

    // Allocate a big chunk of memory to store sax data and positions
    long long hard_buffer_size = (long long)(index->settings->sax_byte_size + index->settings->position_byte_size) * (long long) max_total_buffers_size;
    fbl->hard_buffer = malloc(hard_buffer_size);

    if(fbl->hard_buffer == NULL) {
	fprintf(stderr, "Could not initialize hard buffer of size: %lld\n", hard_buffer_size);
	exit(-1);
    }

    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(fbl_soft_buffer) * number_of_buffers);
    fbl->current_record_index = 0;
    fbl->current_record = fbl->hard_buffer;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
    }
    return fbl;
}

// Botao's version
struct parallel_first_buffer_layer * initialize_pRecBuf(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index) 
{
    struct parallel_first_buffer_layer *fbl = malloc(sizeof(parallel_first_buffer_layer));
    
    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;
    
    // Allocate a big chunk of memory to store sax data and positions
    long long hard_buffer_size = (long long)(index->settings->sax_byte_size + index->settings->position_byte_size) * (long long) max_total_buffers_size;
    //fbl->hard_buffer = malloc(hard_buffer_size);
           
    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(parallel_fbl_soft_buffer) * number_of_buffers);
    fbl->current_record_index = 0;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
        fbl->soft_buffers[i].finished = 0;
    }
    return fbl;
}

// ekosmas version
struct parallel_first_buffer_layer_ekosmas * initialize_pRecBuf_ekosmas(int initial_buffer_size, int number_of_buffers,
                                           int max_total_buffers_size, isax_index *index) 
{
    struct parallel_first_buffer_layer_ekosmas *fbl = malloc(sizeof(parallel_first_buffer_layer_ekosmas));
    
    fbl->max_total_size = max_total_buffers_size;
    fbl->initial_buffer_size = initial_buffer_size;
    fbl->number_of_buffers = number_of_buffers;
    
    // Allocate a big chunk of memory to store sax data and positions
    // long long hard_buffer_size = (long long)(index->settings->sax_byte_size + index->settings->position_byte_size) * (long long) max_total_buffers_size;
    //fbl->hard_buffer = malloc(hard_buffer_size);
           
    // Allocate a set of soft buffers to hold pointers to the hard buffer
    fbl->soft_buffers = malloc(sizeof(parallel_fbl_soft_buffer_ekosmas) * number_of_buffers);
    fbl->current_record_index = 0;
    int i;
    for (i=0; i<number_of_buffers; i++) {
        fbl->soft_buffers[i].initialized = 0;
        fbl->soft_buffers[i].finished = 0;
        fbl->soft_buffers[i].node = NULL;                   // EKOSMAS: ADDED 02 NOVEMBER 2020
    }

    return fbl;
}

void destroy_fbl(first_buffer_layer *fbl) {

    free(fbl->hard_buffer);
    free(fbl->soft_buffers);
    free(fbl);
}
void destroy_pRecBuf(parallel_first_buffer_layer *fbl,int prewokernumber) {

    for (int j=0; j<fbl->number_of_buffers; j++)
    {
         parallel_fbl_soft_buffer *current_fbl_node = &fbl->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }
        for (int k = 0; k < prewokernumber; k++)
        {
            if(current_fbl_node->sax_records[k]!=NULL)
            {
                free((current_fbl_node->sax_records[k]));
                free((current_fbl_node->pos_records[k]));
                current_fbl_node->sax_records[k]=NULL;
                current_fbl_node->pos_records[k]=NULL;
            }
        }
        free(current_fbl_node->buffer_size);
        free(current_fbl_node->max_buffer_size);
        free(current_fbl_node->sax_records);
        free(current_fbl_node->pos_records);
    }
    free(fbl->soft_buffers);
    free(fbl);
}
