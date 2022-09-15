//
//  isax_node_split.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/12/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <pthread.h>

#include "ads/sax/sax.h"
#include "ads/sax/sax_breakpoints.h"
#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_node_split.h"

int informed_split_decision (isax_node_split_data * split_data, 
                             isax_index_settings * settings,
                             isax_node_record * records_buffer,
                             int records_buffer_size) 
{
    double * segment_mean = malloc(sizeof(double) * settings->paa_segments);
    double * segment_stdev = malloc(sizeof(double) * settings->paa_segments);
    
    
    int i,j;
    for(i=0; i<settings->paa_segments; i++)
    {
        segment_mean[i] = 0;
        segment_stdev[i] = 0;
    }
    for (i=0; i<records_buffer_size; i++) 
    {
        for(j=0; j<settings->paa_segments; j++)
        {
            segment_mean[j] += (int)records_buffer[i].sax[j]; 
            
        }
    }
    for(i=0; i<settings->paa_segments; i++)
    {
        segment_mean[i] /= (records_buffer_size); 
        //printf("mean: %lf\n", segment_mean[i]);
    }
    for (i=0; i<records_buffer_size; i++) 
    {
        for(j=0; j<settings->paa_segments; j++)
        {
            segment_stdev[j] += pow(segment_mean[j] - (int)records_buffer[i].sax[j], 2); 
        }
    }
    for(i=0; i<settings->paa_segments; i++)
    {
        segment_stdev[i] = sqrt(segment_stdev[i]/(records_buffer_size)); 
        //printf("stdev: %lf\n", segment_stdev[i]);
    }
    
    
    
    // Decide split point based on the above calculations
    int segment_to_split = -1;
    int segment_to_split_b = -1;
    for(i=0; i<settings->paa_segments; i++)
    {
        if (split_data->split_mask[i] + 1 > settings->sax_bit_cardinality - 1 ) {
            continue;
        }
        else
        {
            // TODO: Optimize this.
            // Calculate break point for new cardinality, a bit complex.
            int new_bit_cardinality = split_data->split_mask[i] + 1;
            int break_point_id = records_buffer[0].sax[i];
            break_point_id = (break_point_id >> ((settings->sax_bit_cardinality) -
                                                (new_bit_cardinality))) << 1;
            int new_cardinality = pow(2, new_bit_cardinality+1);
            int right_offset = ((new_cardinality - 1) * (new_cardinality - 2)) / 2
                                + new_cardinality - 2;
            float b = sax_breakpoints[right_offset - break_point_id];
            
            if (segment_to_split == -1) {
                segment_to_split = i;
                segment_to_split_b = b;
                continue;
            }
            
            float left_range = segment_mean[i] - (3 * segment_stdev[i]);
            float right_range = segment_mean[i] + (3 * segment_stdev[i]);
            //printf("%d, %lf -- %lf \n", i, left_range, right_range);
            
            if(left_range <= b && b <= right_range) {
                if (abs(segment_mean[i] - b) <= abs(segment_mean[i] - segment_to_split_b)) {
                    segment_to_split = i;
                    segment_to_split_b = b;
                }
            }
        }
    }
    
    free(segment_mean);
    free(segment_stdev);
    return segment_to_split;
}

// Manol: Here
void split_node_inmemory(isax_index *index, isax_node *node) 
{
    // ******************************************************* 
    // CREATE TWO NEW NODES AND SET OLD ONE AS AN INTERMEDIATE
    // ******************************************************* 
    int i,sktting;
    
    #ifdef DEBUG    
    printf("*** Splitting. ***\n\n");
    #endif
    
    #ifdef DEBUG
    if (!node->is_leaf) {
        fprintf(stderr,"sanity error: You are trying to split something weird...\
                ARE YOU TRYING TO KILL ME?\n");
    }
    #endif
    
    node->is_leaf = 0;
    node->leaf_size = 0;
    
    // Create split_data for this node.
    isax_node_split_data * split_data = malloc(sizeof(isax_node_split_data));
    if(split_data == NULL) {
        fprintf(stderr,"error: could not allocate memory for node split data.\n");
    }
    split_data->split_mask = malloc(sizeof(sax_type) * index->settings->paa_segments);
    if(split_data->split_mask == NULL) {
        fprintf(stderr,"error: could not allocate memory for node split mask.\n");
    }
    
    if (node->parent == NULL) {
        for (i=0; i<index->settings->paa_segments; i++) {
            split_data->split_mask[i] = 0;
        }
        split_data->splitpoint = 0;
    }
    else {
        for (i=0; i<index->settings->paa_segments; i++) {
            split_data->split_mask[i] = node->parent->split_data->split_mask[i];
        }
    }
    
    index->memory_info.mem_tree_structure += 2;
    
    isax_node * left_child = isax_leaf_node_init(index->settings->initial_leaf_buffer_size, NULL);
    isax_node * right_child = isax_leaf_node_init(index->settings->initial_leaf_buffer_size, NULL);
    
    left_child->is_leaf = 1;
    right_child->is_leaf = 1;
    left_child->parent = node;
    right_child->parent = node;
    node->split_data = split_data;
    node->left_child = left_child;
    node->right_child = right_child;    
    
    // ############ S P L I T   D A T A #############
    // Allocating 1 more position to cover any off-sized allocations happening due to
    // trying to load one more record from a fetched file page which does not exist.
    // e.g. line 284 ( if(!fread... )
    isax_node_record *split_buffer = malloc(sizeof(isax_node_record) * 
                                            (index->settings->max_leaf_size + 1));
    
    int split_buffer_index = 0;
    
    // ********************************************************
    // SPLIT SAX BUFFERS CONTAINED IN *RAM* AND PUT IN CHILDREN
    // ******************************************************** 
    // Split both sax and ts data and move to the new leafs
    
    if(node->buffer->full_buffer_size > 0)
        for (i=node->buffer->full_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->full_sax_buffer[i];
            split_buffer[split_buffer_index].ts = node->buffer->full_ts_buffer[i];
            split_buffer[split_buffer_index].position = node->buffer->full_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = NO_TMP | FULL;
            split_buffer_index++;
        }
    node->buffer->full_buffer_size = 0;
    
    if(node->buffer->partial_buffer_size > 0)
        for (i=node->buffer->partial_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->partial_sax_buffer[i];
            split_buffer[split_buffer_index].ts = NULL;
            split_buffer[split_buffer_index].position = node->buffer->partial_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = NO_TMP | PARTIAL;
            split_buffer_index++;
        }
    node->buffer->partial_buffer_size = 0;
    
    if(node->buffer->tmp_full_buffer_size > 0)
        for (i=node->buffer->tmp_full_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->tmp_full_sax_buffer[i];
            split_buffer[split_buffer_index].ts = node->buffer->tmp_full_ts_buffer[i];
            split_buffer[split_buffer_index].position = node->buffer->tmp_full_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = TMP | FULL;
            split_buffer_index++;
        }
    node->buffer->tmp_full_buffer_size = 0;
    
    if(node->buffer->tmp_partial_buffer_size > 0)
        for (i=node->buffer->tmp_partial_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->tmp_partial_sax_buffer[i];
            split_buffer[split_buffer_index].ts = NULL;
            split_buffer[split_buffer_index].position = node->buffer->tmp_partial_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = TMP | PARTIAL;
            split_buffer_index++;
            
        }
    node->buffer->tmp_partial_buffer_size = 0;
    
    destroy_node_buffer(node->buffer);
    node->buffer = NULL;
    
    
    // *****************************************************
    // SPLIT BUFFERS CONTAINED ON *DISK* AND PUT IN CHILDREN
    // ***************************************************** 
    
    // File is split in two files but it is not
    // removed from disk. It is going to be used in the end.

    
    //printf("sizeof split buffer: %d\n", split_buffer_index);
    // Insert buffered data in children
    
    
    // Decide split point...
    //printf("informed decision: %d\n",
    //       informed_split_decision(split_data, index->settings, split_buffer, (split_buffer_index)));
    
    split_data->splitpoint = informed_split_decision(split_data, index->settings, split_buffer, (split_buffer_index));
    // Decide split point...
    //split_data->splitpoint = simple_split_decision(split_data, index->settings);
    
    
    //printf("not informed decision: %d\n", split_data->splitpoint);
     if(split_data->splitpoint < 0)
     {
         printf("Here 1"); fflush(stdout);
          fprintf(stderr,"error: cannot split in depth more than %d.\n",
                index->settings->sax_bit_cardinality);
        exit(-1);
     }
    
    if(++split_data->split_mask[split_data->splitpoint] > index->settings->sax_bit_cardinality - 1) {
         printf("Here 2"); fflush(stdout);
        fprintf(stderr,"error: cannot split in depth more than %d.\n", 
                index->settings->sax_bit_cardinality);
        exit(-1);
    }
    
    root_mask_type mask = index->settings->bit_masks[index->settings->sax_bit_cardinality - 
                                                     split_data->split_mask[split_data->splitpoint] - 1];
    
    
    
    
    
    while (split_buffer_index > 0) {
        split_buffer_index--;
        if(mask & split_buffer[split_buffer_index].sax[split_data->splitpoint]) {   
            add_record_to_node_inmemory(index, right_child, &split_buffer[split_buffer_index], 1);
        }
        else {
            add_record_to_node_inmemory(index, left_child, &split_buffer[split_buffer_index], 1);
        }
    }
    
    free(split_buffer);
    //printf("Splitted\n");
    
}

void split_node_inmemory_parallel_locks(isax_index *index, isax_node *node)
{
    // ******************************************************* 
    // CREATE TWO NEW NODES AND SET OLD ONE AS AN INTERMEDIATE
    // ******************************************************* 
    int i,sktting;
    
    #ifdef DEBUG    
    printf("*** Splitting. ***\n\n");
    #endif
    
    #ifdef DEBUG
    if (!node->is_leaf) {
        fprintf(stderr,"sanity error: You are trying to split something weird...\
                ARE YOU TRYING TO KILL ME?\n");
    }
    #endif
    
    // node->is_leaf = 0;           // EKOSMAS: MOVED below, JULY 20, 2020, due to problem with add_record_to_node_inmemory_parallel_locks
    // node->leaf_size = 0;         // EKOSMAS: MOVED below, JULY 20, 2020, due to problem with add_record_to_node_inmemory_parallel_locks
    
    // Create split_data for this node.
    isax_node_split_data * split_data = malloc(sizeof(isax_node_split_data));
    if(split_data == NULL) {
        fprintf(stderr,"error: could not allocate memory for node split data.\n");
    }
    split_data->split_mask = malloc(sizeof(sax_type) * index->settings->paa_segments);
    if(split_data->split_mask == NULL) {
        fprintf(stderr,"error: could not allocate memory for node split mask.\n");
    }
    
    if (node->parent == NULL) {
        for (i=0; i<index->settings->paa_segments; i++) {
            split_data->split_mask[i] = 0;
        }
        split_data->splitpoint = 0;
    }
    else {
        for (i=0; i<index->settings->paa_segments; i++) {
            split_data->split_mask[i] = node->parent->split_data->split_mask[i];
        }
    }
    
    index->memory_info.mem_tree_structure += 2;
    
    isax_node * left_child = isax_leaf_node_init(index->settings->initial_leaf_buffer_size, NULL);
    isax_node * right_child = isax_leaf_node_init(index->settings->initial_leaf_buffer_size, NULL);
    
    left_child->is_leaf = 1;
    right_child->is_leaf = 1;
    left_child->parent = node;
    right_child->parent = node;
    node->split_data = split_data;
    node->left_child = left_child;
    node->right_child = right_child;
    
    left_child->lock_node = malloc (sizeof(pthread_mutex_t));
    pthread_mutex_init (left_child->lock_node, NULL);
    right_child->lock_node = malloc (sizeof(pthread_mutex_t));
    pthread_mutex_init (right_child->lock_node, NULL);
    
    
    // ############ S P L I T   D A T A #############
    // Allocating 1 more position to cover any off-sized allocations happening due to
    // trying to load one more record from a fetched file page which does not exist.
    // e.g. line 284 ( if(!fread... )
    isax_node_record *split_buffer = malloc(sizeof(isax_node_record) * 
                                            (index->settings->max_leaf_size + 1));
    
    int split_buffer_index = 0;
    
    
    
    // ********************************************************
    // SPLIT SAX BUFFERS CONTAINED IN *RAM* AND PUT IN CHILDREN
    // ******************************************************** 
    // Split both sax and ts data and move to the new leafs
    
    if(node->buffer->full_buffer_size > 0)
        for (i=node->buffer->full_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->full_sax_buffer[i];
            split_buffer[split_buffer_index].ts = node->buffer->full_ts_buffer[i];
            split_buffer[split_buffer_index].position = node->buffer->full_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = NO_TMP | FULL;
            split_buffer_index++;
        }
    node->buffer->full_buffer_size = 0;
    
    if(node->buffer->partial_buffer_size > 0)
        for (i=node->buffer->partial_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->partial_sax_buffer[i];
            split_buffer[split_buffer_index].ts = NULL;
            split_buffer[split_buffer_index].position = node->buffer->partial_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = NO_TMP | PARTIAL;
            split_buffer_index++;
        }
    node->buffer->partial_buffer_size = 0;
    
    if(node->buffer->tmp_full_buffer_size > 0)
        for (i=node->buffer->tmp_full_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->tmp_full_sax_buffer[i];
            split_buffer[split_buffer_index].ts = node->buffer->tmp_full_ts_buffer[i];
            split_buffer[split_buffer_index].position = node->buffer->tmp_full_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = TMP | FULL;
            split_buffer_index++;
        }
    node->buffer->tmp_full_buffer_size = 0;
    
    if(node->buffer->tmp_partial_buffer_size > 0)
        for (i=node->buffer->tmp_partial_buffer_size-1; i>=0; i--) {
            split_buffer[split_buffer_index].sax = node->buffer->tmp_partial_sax_buffer[i];
            split_buffer[split_buffer_index].ts = NULL;
            split_buffer[split_buffer_index].position = node->buffer->tmp_partial_position_buffer[i];
            split_buffer[split_buffer_index].insertion_mode = TMP | PARTIAL;
            split_buffer_index++;
            
        }
    node->buffer->tmp_partial_buffer_size = 0;
    
    destroy_node_buffer(node->buffer);
    node->buffer = NULL;
    
    
    // *****************************************************
    // SPLIT BUFFERS CONTAINED ON *DISK* AND PUT IN CHILDREN
    // ***************************************************** 
    
    // File is split in two files but it is not
    // removed from disk. It is going to be used in the end.

    
    //printf("sizeof split buffer: %d\n", split_buffer_index);
    // Insert buffered data in children
    
    
    // Decide split point...
    //printf("informed decision: %d\n",
    //       informed_split_decision(split_data, index->settings, split_buffer, (split_buffer_index)));
    
    split_data->splitpoint = informed_split_decision(split_data, index->settings, split_buffer, (split_buffer_index));
    // Decide split point...
    //split_data->splitpoint = simple_split_decision(split_data, index->settings);
    
    
    //printf("not informed decision: %d\n", split_data->splitpoint);
     if(split_data->splitpoint < 0)
     {
          fprintf(stderr,"error: cannot split in depth more than %d.\n",
                index->settings->sax_bit_cardinality);
        exit(-1);
     }
    
    if(++split_data->split_mask[split_data->splitpoint] > index->settings->sax_bit_cardinality - 1) {
        fprintf(stderr,"error: cannot split in depth more than %d.\n", 
                index->settings->sax_bit_cardinality);
        exit(-1);
    }
    
    root_mask_type mask = index->settings->bit_masks[index->settings->sax_bit_cardinality - 
                                                     split_data->split_mask[split_data->splitpoint] - 1];
    
    
    
    
    
    while (split_buffer_index > 0) {
        split_buffer_index--;
        if(mask & split_buffer[split_buffer_index].sax[split_data->splitpoint]) {   
            add_record_to_node_inmemory_parallel_locks(index, right_child, &split_buffer[split_buffer_index]);
        }
        else {
            add_record_to_node_inmemory_parallel_locks(index, left_child, &split_buffer[split_buffer_index]);
        }
    }
    
    free(split_buffer);
    //printf("Splitted\n");   

    node->is_leaf = 0;                  // EKOSMAS: MOVED, JULY 20, 2020, due to problem with add_record_to_node_inmemory_parallel_locks
    node->leaf_size = 0;                // EKOSMAS: MOVED, JULY 20, 2020, due to problem with add_record_to_node_inmemory_parallel_locks
}
