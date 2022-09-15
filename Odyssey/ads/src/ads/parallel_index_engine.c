//
//  parallel_index_engine.c
//
//
//  Created by Botao PENG on 29/1/18.
//


#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <pthread.h>
#include <unistd.h>
//#include <semaphore.h>
#include <stdbool.h>

#include "ads/isax_node.h"
#include "ads/isax_index.h"
#include "ads/isax_query_engine.h"
#include "ads/isax_node_record.h"
#include "ads/isax_file_loaders.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/parallel_index_engine.h"
#include "ads/parallel_query_engine.h"


isax_node * insert_to_pRecBuf(parallel_first_buffer_layer *fbl, sax_type *sax,
                          file_position_type *pos,root_mask_type mask,
                          isax_index *index, pthread_mutex_t *lock_firstnode, int workernumber,int total_workernumber)
{
    //pthread_rwlock_wrlock(lockfbl);
    parallel_fbl_soft_buffer *current_buffer = &fbl->soft_buffers[(int) mask];

    file_position_type *filepointer;
    sax_type *saxpointer;

    int current_buffer_number;
    char *cd_s,*cd_p;

    // Check if this buffer is initialized
    if (!current_buffer->initialized)
    {
        pthread_mutex_lock(lock_firstnode); //not initialized twice -> may small buffers then realloc if needed -compare time with lock and with initialized bufs
        if (!current_buffer->initialized)
        {

            current_buffer->max_buffer_size = malloc(sizeof(int)*total_workernumber);
            current_buffer->buffer_size = malloc(sizeof(int)*total_workernumber);
            current_buffer->sax_records = malloc(sizeof(sax_type *)*total_workernumber);
            current_buffer->pos_records = malloc(sizeof(file_position_type *)*total_workernumber);
            for (int i = 0; i < total_workernumber; i++)
            {
                current_buffer->max_buffer_size[i]=0;
                current_buffer->buffer_size[i]=0;
                current_buffer->pos_records[i]=NULL;
                current_buffer->sax_records[i]=NULL;
            }
            current_buffer->node = isax_root_node_init(mask,index->settings->initial_leaf_buffer_size, NULL);
            current_buffer->node->is_leaf = 1;
            //current_buffer->finished=1;
            current_buffer->initialized = 1;
            //__sync_synchronize();
            if(index->first_node == NULL)
            {
                index->first_node = (isax_node *)current_buffer->node;
                // pthread_mutex_unlock(lock_firstnode);            // EKOSMAS: (RE)MOVED JUNE 03 2020
                current_buffer->node->next = NULL;
                current_buffer->node->previous = NULL;
                pthread_mutex_unlock(lock_firstnode);               // EKOSMAS: ADDED (/MOVED) JUNE 03 2020

            }
            else
            {
                isax_node * prev_first = index->first_node;
                index->first_node = (isax_node *) current_buffer->node;
                index->first_node->next = prev_first;
                prev_first->previous = (isax_node *) current_buffer->node;
                pthread_mutex_unlock(lock_firstnode);
            }
            __sync_fetch_and_add(&(index->root_nodes),1);           // EKOSMAS: Is this FAA required? Can I move it inside critical section (of lock_firstnode) and still be correct?
        }
        else
        {
           pthread_mutex_unlock(lock_firstnode);
        }
    }

    // Check if this buffer is not full!
    if (current_buffer->buffer_size[workernumber] >= current_buffer->max_buffer_size[workernumber]) {
        if(current_buffer->max_buffer_size[workernumber] == 0) {
            current_buffer->max_buffer_size[workernumber] = fbl->initial_buffer_size;
            current_buffer->sax_records[workernumber] = malloc(index->settings->sax_byte_size *
                                                 current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = malloc(index->settings->position_byte_size*
                                                 current_buffer->max_buffer_size[workernumber]);
        }
        else {
            current_buffer->max_buffer_size[workernumber] *= BUFFER_REALLOCATION_RATE;

            current_buffer->sax_records[workernumber] = realloc(current_buffer->sax_records[workernumber],
                                           index->settings->sax_byte_size *
                                           current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = realloc(current_buffer->pos_records[workernumber],
                                           index->settings->position_byte_size *
                                           current_buffer->max_buffer_size[workernumber]);

        }
    }

    if (current_buffer->sax_records[workernumber] == NULL || current_buffer->pos_records[workernumber] == NULL) {
        fprintf(stderr, "error: Could not allocate memory in FBL.");
        return OUT_OF_MEMORY_FAILURE;
    }
    
    // Copy data to hard buffer and make current buffer point to the hard one
    //pthread_mutex_lock(lockfbl);
    //COUNT_CAL_TIME_START
    //fbl->current_record_index++;

    //cd_s=fbl->current_record;
    //fbl->current_record += index->settings->sax_byte_size;
    //cd_s= __sync_fetch_and_add(&(fbl->current_record),index->settings->sax_byte_size+index->settings->position_byte_size);
    //cd_p=fbl->current_record;
    //fbl->current_record += index->settings->position_byte_size;
    //cd_p= cd_s+index->settings->sax_byte_size;
    //pthread_mutex_unlock(lockfbl);

    current_buffer_number = current_buffer->buffer_size[workernumber];
    filepointer = (file_position_type *)current_buffer->pos_records[workernumber];
    saxpointer = (sax_type *)current_buffer->sax_records[workernumber];
    //printf("the work number is %d sax is  %d \n",workernumber,saxpointer[current_buffer_number*index->settings->paa_segments]);
    // EKOSMAS: we could avoid these memcopies and dereferences of pointers by having: i) sax_type *sax_records and ii) file_position_type *pos_records, insted pf **
    memcpy((void *) (&saxpointer[current_buffer_number*index->settings->paa_segments]), (void *) sax, index->settings->sax_byte_size);
    memcpy((void *) (&filepointer[current_buffer_number]), (void *) pos, index->settings->position_byte_size);


    #ifdef DEBUG
    printf("*** Added to node ***\n\n");
    #ifdef TOY
    sax_print(sax, index->settings->paa_segments,
              index->settings->sax_bit_cardinality);
    #endif
    #endif
    //printf("this is befor the checke \n");
    //__sync_fetch_and_add(&((current_buffer->buffer_size[workernumber])),1);
    //printf("this is after  the checke \n");
    (current_buffer->buffer_size[workernumber])++;

    return (isax_node *) current_buffer->node;
}
isax_node * insert_to_pRecBuf_ekosmas(parallel_first_buffer_layer_ekosmas *fbl, sax_type *sax,
                          file_position_type *pos,root_mask_type mask,
                          isax_index *index, pthread_mutex_t *lock_firstnode, int workernumber,int total_workernumber)
{
    //pthread_rwlock_wrlock(lockfbl);
    parallel_fbl_soft_buffer_ekosmas *current_buffer = &fbl->soft_buffers[(int) mask];

    file_position_type *filepointer;
    sax_type *saxpointer;

    int current_buffer_number;
    char *cd_s,*cd_p;

    // Check if this buffer is initialized
    if (!current_buffer->initialized)
    {
        pthread_mutex_lock(lock_firstnode); //not initialized twice -> may small buffers then realloc if needed -compare time with lock and with initialized bufs
        if (!current_buffer->initialized)
        {

            current_buffer->max_buffer_size = malloc(sizeof(int)*total_workernumber);
            current_buffer->buffer_size = malloc(sizeof(int)*total_workernumber);
            current_buffer->sax_records = malloc(sizeof(sax_type *)*total_workernumber);
            current_buffer->pos_records = malloc(sizeof(file_position_type *)*total_workernumber);
            for (int i = 0; i < total_workernumber; i++)
            {
                current_buffer->max_buffer_size[i]=0;
                current_buffer->buffer_size[i]=0;
                current_buffer->pos_records[i]=NULL;
                current_buffer->sax_records[i]=NULL;
            }
            current_buffer->node = isax_root_node_init(mask,index->settings->initial_leaf_buffer_size, NULL);
            current_buffer->node->is_leaf = 1;
            current_buffer->initialized = 1;

            // if(index->first_node == NULL)
            // {
            //     index->first_node = (isax_node *)current_buffer->node;
            //     // pthread_mutex_unlock(lock_firstnode);            // EKOSMAS: (RE)MOVED JUNE 03 2020
            //     current_buffer->node->next = NULL;
            //     current_buffer->node->previous = NULL;
            //     pthread_mutex_unlock(lock_firstnode);               // EKOSMAS: ADDED (/MOVED) JUNE 03 2020

            // }
            // else
            // {
            //     isax_node * prev_first = index->first_node;
            //     index->first_node = (isax_node *) current_buffer->node;
            //     index->first_node->next = prev_first;
            //     prev_first->previous = (isax_node *) current_buffer->node;
            //     pthread_mutex_unlock(lock_firstnode);
            // }
            // __sync_fetch_and_add(&(index->root_nodes),1);           // EKOSMAS: Is this FAA required? Can I move it inside critical section (of lock_firstnode) and still be correct?
        }
        // else
        // {
        //    pthread_mutex_unlock(lock_firstnode);
        // }
        pthread_mutex_unlock(lock_firstnode);
    }

    // Check if this buffer is not full!
    if (current_buffer->buffer_size[workernumber] >= current_buffer->max_buffer_size[workernumber]) {
        if(current_buffer->max_buffer_size[workernumber] == 0) {
            current_buffer->max_buffer_size[workernumber] = fbl->initial_buffer_size;
            current_buffer->sax_records[workernumber] = malloc(index->settings->sax_byte_size *
                                                 current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = malloc(index->settings->position_byte_size*
                                                 current_buffer->max_buffer_size[workernumber]);
        }
        else {
            current_buffer->max_buffer_size[workernumber] *= BUFFER_REALLOCATION_RATE;

            current_buffer->sax_records[workernumber] = realloc(current_buffer->sax_records[workernumber],
                                           index->settings->sax_byte_size *
                                           current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = realloc(current_buffer->pos_records[workernumber],
                                           index->settings->position_byte_size *
                                           current_buffer->max_buffer_size[workernumber]);

        }
    }

    if (current_buffer->sax_records[workernumber] == NULL || current_buffer->pos_records[workernumber] == NULL) {
        fprintf(stderr, "error: Could not allocate memory in FBL.");
        return OUT_OF_MEMORY_FAILURE;
    }
    
    // Copy data to hard buffer and make current buffer point to the hard one
    //pthread_mutex_lock(lockfbl);
    //COUNT_CAL_TIME_START
    //fbl->current_record_index++;

    //cd_s=fbl->current_record;
    //fbl->current_record += index->settings->sax_byte_size;
    //cd_s= __sync_fetch_and_add(&(fbl->current_record),index->settings->sax_byte_size+index->settings->position_byte_size);
    //cd_p=fbl->current_record;
    //fbl->current_record += index->settings->position_byte_size;
    //cd_p= cd_s+index->settings->sax_byte_size;
    //pthread_mutex_unlock(lockfbl);

    current_buffer_number = current_buffer->buffer_size[workernumber];
    filepointer = (file_position_type *)current_buffer->pos_records[workernumber];
    saxpointer = (sax_type *)current_buffer->sax_records[workernumber];
    //printf("the work number is %d sax is  %d \n",workernumber,saxpointer[current_buffer_number*index->settings->paa_segments]);
    // EKOSMAS: we could avoid these memcopies and dereferences of pointers by having: i) sax_type *sax_records and ii) file_position_type *pos_records, insted pf **
    memcpy((void *) (&saxpointer[current_buffer_number*index->settings->paa_segments]), (void *) sax, index->settings->sax_byte_size);
    memcpy((void *) (&filepointer[current_buffer_number]), (void *) pos, index->settings->position_byte_size);


    #ifdef DEBUG
    printf("*** Added to node ***\n\n");
    #ifdef TOY
    sax_print(sax, index->settings->paa_segments,
              index->settings->sax_bit_cardinality);
    #endif
    #endif
    //printf("this is befor the checke \n");
    //__sync_fetch_and_add(&((current_buffer->buffer_size[workernumber])),1);
    //printf("this is after  the checke \n");
    (current_buffer->buffer_size[workernumber])++;

    return (isax_node *) current_buffer->node;
}
isax_node * insert_to_2pRecBuf(parallel_dfirst_buffer_layer *fbl, sax_type *sax,
                          file_position_type *pos,root_mask_type mask,
                          isax_index *index, pthread_mutex_t *lock_firstnode, int workernumber,int total_workernumber)
{
    //pthread_rwlock_wrlock(lockfbl);
    parallel_dfbl_soft_buffer *current_buffer = &fbl->soft_buffers[(int) mask];

    file_position_type **filepointer;
    sax_type **saxpointer;

    int current_buffer_number;
    char * cd_s,*cd_p;
    // Check if this buffer is initialized


    if (!current_buffer->initialized)
    {
        pthread_mutex_lock(lock_firstnode);
        if (!current_buffer->initialized)
        {

            current_buffer->max_buffer_size = malloc(sizeof(int)*total_workernumber);
            current_buffer->buffer_size = malloc(sizeof(int)*total_workernumber);
            current_buffer->sax_records=malloc(sizeof(sax_type **)*total_workernumber);
            current_buffer->pos_records=malloc(sizeof(file_position_type **)*total_workernumber);
            for (int i = 0; i < total_workernumber; i++)
            {
                current_buffer->max_buffer_size[i]=0;
                current_buffer->buffer_size[i]=0;
                current_buffer->pos_records[i]=NULL;
                current_buffer->sax_records[i]=NULL;
            }
            current_buffer->node = isax_root_node_init(mask,index->settings->initial_leaf_buffer_size, NULL);
            current_buffer->node->is_leaf = 1;
            //current_buffer->finished=1;
            current_buffer->initialized = 1;
            //__sync_synchronize();
            if(index->first_node == NULL)
            {
                index->first_node = current_buffer->node;
                pthread_mutex_unlock(lock_firstnode);
                current_buffer->node->next = NULL;
                current_buffer->node->previous = NULL;

            }
            else
            {
                isax_node * prev_first = index->first_node;
                index->first_node = current_buffer->node;
                index->first_node->next = prev_first;
                prev_first->previous = current_buffer->node;
                pthread_mutex_unlock(lock_firstnode);
            }
            __sync_fetch_and_add(&(index->root_nodes),1);
        }
        else
        {
           pthread_mutex_unlock(lock_firstnode);
        }
    }

    // Check if this buffer is not full!
    if (current_buffer->buffer_size[workernumber] >= current_buffer->max_buffer_size[workernumber]) {
        if(current_buffer->max_buffer_size[workernumber] == 0) {
            current_buffer->max_buffer_size[workernumber] = fbl->initial_buffer_size;
            current_buffer->sax_records[workernumber] = malloc(sizeof(sax_type *) *
                                                 current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = malloc(sizeof(file_position_type *)*
                                                 current_buffer->max_buffer_size[workernumber]);
        }
        else {
            current_buffer->max_buffer_size[workernumber] *= BUFFER_REALLOCATION_RATE;

            current_buffer->sax_records[workernumber] = realloc(current_buffer->sax_records[workernumber],
                                           sizeof(sax_type *) *
                                           current_buffer->max_buffer_size[workernumber]);
            current_buffer->pos_records[workernumber] = realloc(current_buffer->pos_records[workernumber],
                                           sizeof(file_position_type *) *
                                           current_buffer->max_buffer_size[workernumber]);

        }
    }

    if (current_buffer->sax_records[workernumber] == NULL || current_buffer->pos_records[workernumber] == NULL) {
        fprintf(stderr, "error: Could not allocate memory in FBL.");
        return OUT_OF_MEMORY_FAILURE;
    }
    // Copy data to hard buffer and make current buffer point to the hard one
    //pthread_mutex_lock(lockfbl);
    //COUNT_CAL_TIME_START
    //fbl->current_record_index++;

    //cd_s=fbl->current_record;
    //fbl->current_record += index->settings->sax_byte_size;
    //cd_s= __sync_fetch_and_add(&(fbl->current_record),index->settings->sax_byte_size+index->settings->position_byte_size);
    //cd_p=fbl->current_record;
    //fbl->current_record += index->settings->position_byte_size;
    //cd_p= cd_s+index->settings->sax_byte_size;
    //pthread_mutex_unlock(lockfbl);
    current_buffer_number=current_buffer->buffer_size[workernumber];
    filepointer=(file_position_type **)current_buffer->pos_records[workernumber];
    saxpointer=(sax_type **)current_buffer->sax_records[workernumber];
    saxpointer[current_buffer_number]=((parallel_dfirst_buffer_layer*)index->fbl)->hard_buffer+*pos/sizeof(ts_type)/index->settings->timeseries_size*index->settings->sax_byte_size;
    filepointer[current_buffer_number]= (file_position_type *)((parallel_dfirst_buffer_layer*)index->fbl)->hard_buffer+*pos/sizeof(ts_type)/index->settings->timeseries_size*index->settings->sax_byte_size+index->settings->sax_byte_size;
    //printf("the work number is %d sax is  %d \n",workernumber,saxpointer[current_buffer_number*index->settings->paa_segments]);
    //memcpy((void *) (&saxpointer[current_buffer_number]), (void *) ((parallel_dfirst_buffer_layer*)index->fbl)->hard_buffer+*pos/sizeof(ts_type)/index->settings->timeseries_size*index->settings->sax_byte_size , sizeof(sax_type *));
    //memcpy((void *) (&filepointer[current_buffer_number]), (void *) ((parallel_dfirst_buffer_layer*)index->fbl)->hard_buffer+*pos/sizeof(ts_type)/index->settings->timeseries_size*index->settings->sax_byte_size+index->settings->sax_byte_size, sizeof(file_position_type *));

    #ifdef DEBUG
    printf("*** Added to node ***\n\n");
    #ifdef TOY
    sax_print(sax, index->settings->paa_segments,
              index->settings->sax_bit_cardinality);
    #endif
    #endif
    //printf("this is befor the checke \n");
    //__sync_fetch_and_add(&((current_buffer->buffer_size[workernumber])),1);
    //printf("this is after  the checke \n");
    (current_buffer->buffer_size[workernumber])++;

    return current_buffer->node;
}
