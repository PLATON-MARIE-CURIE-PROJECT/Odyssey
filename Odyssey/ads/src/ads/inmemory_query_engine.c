#ifdef VALUES
#include <values.h>
#endif
#include <float.h>
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include "ads/isax_query_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"

query_result approximate_search_inmemory_pRecBuf(ts_type *ts, ts_type *paa, isax_index *index)
{
    query_result result;

    sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    sax_from_paa(paa, sax, index->settings->paa_segments,
                 index->settings->sax_alphabet_cardinality,
                 index->settings->sax_bit_cardinality);

    root_mask_type root_mask = 0;
    CREATE_MASK(root_mask, index, sax);

    if ((&((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[(int)root_mask])->initialized)
    {
        isax_node *node = (isax_node *)(&((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[(int)root_mask])->node;
        // Traverse tree

        // Adaptive splitting

        while (!node->is_leaf)
        {
            int location = index->settings->sax_bit_cardinality - 1 -
                           node->split_data->split_mask[node->split_data->splitpoint];
            root_mask_type mask = index->settings->bit_masks[location];

            if (sax[node->split_data->splitpoint] & mask)
            {
                node = node->right_child;
            }
            else
            {
                node = node->left_child;
            }

            // Adaptive splitting
        }
        result.distance = calculate_node_distance_inmemory(index, node, ts, FLT_MAX); // caclulate initial BSF
        result.node = node;
    }
    else
    {
        printf("approximate_search_inmemory_pRecBuf: NO BSF has been computed! Bad Luck...\n");
        fflush(stdout);
        result.node = NULL;
        result.distance = FLT_MAX;
    }

    free(sax);

    return result;
}

float approximate_search_inmemory_pRecBuf_subtree_root_chatzakis(ts_type *ts, ts_type *paa, isax_index *index, isax_node *subtree_root)
{
    isax_node *node = subtree_root;

    if (subtree_root == NULL)
    {
        return FLT_MAX;
    }

    while (!node->is_leaf)
    {
        //should never be null!
        isax_node *left_c = node->left_child;
        isax_node *right_c = node->right_child;

        float dist1 = FLT_MAX, dist2 = FLT_MAX;

        if (left_c && left_c->isax_values && left_c->isax_cardinalities)
        {
            dist1 = minidist_paa_to_isax(paa, left_c->isax_values,
                                         left_c->isax_cardinalities,
                                         index->settings->sax_bit_cardinality,
                                         index->settings->sax_alphabet_cardinality,
                                         index->settings->paa_segments,
                                         MINVAL, MAXVAL,
                                         index->settings->mindist_sqrt);
        }

        if (right_c && right_c->isax_values && right_c->isax_cardinalities)
        {
            dist2 = minidist_paa_to_isax(paa, right_c->isax_values,
                                         right_c->isax_cardinalities,
                                         index->settings->sax_bit_cardinality,
                                         index->settings->sax_alphabet_cardinality,
                                         index->settings->paa_segments,
                                         MINVAL, MAXVAL,
                                         index->settings->mindist_sqrt);
        }

        if (dist1 > dist2)
        {
            node = right_c;
        }
        else
        {
            node = left_c;
        }
    }

    return calculate_node_distance_inmemory_ekosmas(index, node, ts, FLT_MAX);
}

query_result approximate_search_inmemory_pRecBuf_ekosmas(ts_type *ts, ts_type *paa, isax_index *index)
{
    query_result result;

    sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    sax_from_paa(paa, sax, index->settings->paa_segments, index->settings->sax_alphabet_cardinality, index->settings->sax_bit_cardinality);

    root_mask_type root_mask = 0;
    CREATE_MASK(root_mask, index, sax);

    if ((&((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[(int)root_mask])->initialized)
    {
        isax_node *node = (isax_node *)(&((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[(int)root_mask])->node;

        // Traverse tree
        while (!node->is_leaf)
        {
            int location = index->settings->sax_bit_cardinality - 1 - node->split_data->split_mask[node->split_data->splitpoint];
            root_mask_type mask = index->settings->bit_masks[location];

            if (sax[node->split_data->splitpoint] & mask)
            {
                node = node->right_child;
            }
            else
            {
                node = node->left_child;
            }
        }

        result.distance = calculate_node_distance_inmemory_ekosmas(index, node, ts, FLT_MAX); // caclulate initial BSF
        result.node = node;
    }
    else
    {
        printf("approximate_search_inmemory_pRecBuf_ekosmas: NO BSF has been computed! Bad Luck...\n");
        fflush(stdout);
        result.node = NULL;
        result.distance = FLT_MAX;
    }

    free(sax);

    return result;
}

float calculate_node_distance_inmemory(isax_index *index, isax_node *node, ts_type *query, float bsf)
{
    COUNT_CHECKED_NODE()
    // If node has buffered data

    if (node->buffer != NULL)
    {
        int i;
        for (i = 0; i < node->buffer->full_buffer_size; i++)
        {
            float dist = ts_euclidean_distance(query, node->buffer->full_ts_buffer[i],
                                               index->settings->timeseries_size, bsf);
            if (dist < bsf)
            {
                bsf = dist;
            }
        }

        for (i = 0; i < node->buffer->tmp_full_buffer_size; i++)
        {
            float dist = ts_euclidean_distance(query, node->buffer->tmp_full_ts_buffer[i],
                                               index->settings->timeseries_size, bsf);
            if (dist < bsf)
            {
                bsf = dist;
            }
        }
        // RDcalculationnumber=RDcalculationnumber+node->buffer->partial_buffer_size;
        for (i = 0; i < node->buffer->partial_buffer_size; i++)
        {

            float dist = ts_euclidean_distance_SIMD(query, &(tmp_raw_file[*node->buffer->partial_position_buffer[i]]), // refer to global raw_file
                                                    index->settings->timeseries_size, bsf);

            if (dist < bsf)
            {
                bsf = dist;
            }
        }
    }

    return bsf;
}

float calculate_node_distance_inmemory_ekosmas(isax_index *index, isax_node *node, ts_type *query, float bsf)
{
    // If node has buffered data
    if (node->buffer != NULL)
    {
        for (int i = 0; i < node->buffer->partial_buffer_size; i++)
        {

            float dist = ts_euclidean_distance_SIMD(query, &(tmp_raw_file[*node->buffer->partial_position_buffer[i]]), index->settings->timeseries_size, bsf); // refer to global raw_file

            if (dist < bsf)
            {
                // printf("=== === [MC] -- Updating BSF from %f to %f", bsf, dist);
                bsf = dist;
            }
        }
    }

    return bsf;
}

float calculate_node_distance2_inmemory(isax_index *index, isax_node *node, ts_type *query, ts_type *paa, float bsf)
{
    COUNT_CHECKED_NODE()
    float distmin;
    // If node has buffered data
    if (node->buffer != NULL)
    {
        int i;

        for (i = 0; i < node->buffer->partial_buffer_size; i++)
        {

            distmin = minidist_paa_to_isax_rawa_SIMD(paa, node->buffer->partial_sax_buffer[i],
                                                     index->settings->max_sax_cardinalities,
                                                     index->settings->sax_bit_cardinality,
                                                     index->settings->sax_alphabet_cardinality,
                                                     index->settings->paa_segments, MINVAL, MAXVAL,
                                                     index->settings->mindist_sqrt);
            if (distmin < bsf)
            {
                float dist = ts_euclidean_distance_SIMD(query,
                                                        &(tmp_raw_file[*node->buffer->partial_position_buffer[i]]), // refer to global raw_file
                                                        index->settings->timeseries_size,
                                                        bsf);
                if (dist < bsf)
                {
                    bsf = dist;
                }
            }
        }
    }

    return bsf;
}

extern unsigned __thread long long int thread_real_distance_counter; // per query
extern unsigned __thread long long int thread_min_distance_counter;  // per query

void share_bsf(MESSI_workerdata_ekosmas *input_data);

float calculate_node_distance2_inmemory_ekosmas(isax_index *index, isax_node *node, ts_type *query, ts_type *paa, float bsf, MESSI_workerdata_ekosmas *input_data, query_result *bsf_result)
{

    // if(input_data->workernumber == 0){
    //     printf("Here : node->buffer->partial_buffer_size (%d)\n", node->buffer->partial_buffer_size);
    // }

    // If node has buffered data
    if (node->buffer != NULL)
    {
        for (int i = 0; i < node->buffer->partial_buffer_size; i++)
        {

            // Na rotiso ton Leyteri! 07-02-2021
            if (bsf > bsf_result->distance)
            {
                bsf = bsf_result->distance;
            }

            float distmin = minidist_paa_to_isax_rawa_SIMD(paa, node->buffer->partial_sax_buffer[i],
                                                           index->settings->max_sax_cardinalities,
                                                           index->settings->sax_bit_cardinality,
                                                           index->settings->sax_alphabet_cardinality,
                                                           index->settings->paa_segments, MINVAL, MAXVAL,
                                                           index->settings->mindist_sqrt);
            // to be sure
            // if(thread_min_distance_counter == 0){
            // thread_min_distance_counter += 3;
            // }
            thread_min_distance_counter++;

            if (distmin < bsf)
            {

                // to be sure
                // if(thread_real_distance_counter == 0){
                // thread_real_distance_counter += 2;
                // }
                thread_real_distance_counter++;

                float dist = ts_euclidean_distance_SIMD(query,
                                                        &(tmp_raw_file[*node->buffer->partial_position_buffer[i]]), // refer to global raw_file
                                                        index->settings->timeseries_size,
                                                        bsf);
                if (dist < bsf)
                {
                    bsf = dist;
                }
            }
            if (i % 300 == 0)
            {
                share_bsf(input_data);
            }
        }
    }

    return bsf;
}

float calculate_node_distance2_inmemory_chatzakis(isax_index *index, isax_node *node, ts_type *query, ts_type *paa, float bsf, MESSI_workstealing_query_data_chatzakis *input_data, query_result *bsf_result)
{
    if (node->buffer != NULL)
    {
        for (int i = 0; i < node->buffer->partial_buffer_size; i++)
        {
            if (bsf > bsf_result->distance)
            {
                bsf = bsf_result->distance;
            }

            float distmin = minidist_paa_to_isax_rawa_SIMD(paa, node->buffer->partial_sax_buffer[i],
                                                           index->settings->max_sax_cardinalities,
                                                           index->settings->sax_bit_cardinality,
                                                           index->settings->sax_alphabet_cardinality,
                                                           index->settings->paa_segments, MINVAL, MAXVAL,
                                                           index->settings->mindist_sqrt);

            thread_min_distance_counter++;

            if (distmin < bsf)
            {
                thread_real_distance_counter++;

                float dist = ts_euclidean_distance_SIMD(query, &(tmp_raw_file[*node->buffer->partial_position_buffer[i]]), index->settings->timeseries_size, bsf);
                if (dist < bsf)
                {
                    bsf = dist;
                }
            }

            /*if (i % 300 == 0)
            {
                share_bsf(input_data);
            }*/
        }
    }

    return bsf;
}
