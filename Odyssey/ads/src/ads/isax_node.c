//
//  isax_node.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/10/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>

#include "ads/isax_node.h"

/**
 This function initializes an isax root node.
 */
// EKOSMAS: FUNCTION READ
isax_node * isax_root_node_init(root_mask_type mask, int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, current_fbl_node);
    node->mask = mask;
    return node;
}

/**
 This function initalizes an isax leaf node.
 */
isax_node * isax_leaf_node_init(int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    // COUNT_NEW_NODE()                                                         // EKOSMAS: REMOVED JUNE 17 2020
    isax_node *node = malloc(sizeof(isax_node));
    if(node == NULL) {
        fprintf(stderr,"error: could not allocate memory for new node.\n");
        return NULL;
    }
    node->has_partial_data_file = 0;
    node->has_full_data_file = 0;
    node->right_child = NULL;
    node->left_child = NULL;
    node->parent = NULL;
    node->next = NULL;
    node->leaf_size = 0;
    node->filename = NULL;
    node->isax_values = NULL;
    node->isax_cardinalities = NULL;
    node->previous = NULL;
    node->split_data = NULL;
    node->buffer = init_node_buffer(initial_buffer_size);
    node->fai_leaf_size = 0;                                                    // EKOSMAS: ADDED 28 JULY 2020
    node->mask = 0;
    node->wedges = NULL;
    node->is_leaf = 0;
    node->lightweight_path = 0;
    node->announce_array = NULL;
    node->recBuf_leaf_helpers_exist = 0;
    node->fbl_node = current_fbl_node;
    node->processed = 0;
    node->lock_node = NULL;                                                     // EKOSMAS: ADDED 02 NOVEMBER 2020

    return node;
}

announce_rec *create_new_announce_rec(isax_node_record *record) {
    announce_rec *new_rec = malloc (sizeof(announce_rec));
    new_rec->record.sax = record->sax;
    new_rec->record.position = record->position;
    new_rec->buf_pos = ULONG_MAX;
    return (new_rec);
}
