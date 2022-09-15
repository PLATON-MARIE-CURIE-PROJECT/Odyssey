//
//  Updated by Eleftherios Kosmas on May 2020.
//

#define _GNU_SOURCE

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
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"
#include <sched.h>

extern int query_mode;
extern int my_rank;
extern int comm_sz;
extern int mpi_already_splitted_dataset;
extern int time_series_size;
extern int all_nodes_index_all_given_dataset;
extern int file_part_to_load;

inline void threadPin(int pid, int max_threads)
{
    int cpu_id;

    cpu_id = pid % max_threads;
    pthread_setconcurrency(max_threads);

    cpu_set_t mask;
    unsigned int len = sizeof(mask);

    CPU_ZERO(&mask);

    CPU_SET(cpu_id % max_threads, &mask); // OLD PINNING 1

    // if (cpu_id % 2 == 0)                                             // OLD PINNING 2
    //    CPU_SET(cpu_id % max_threads, &mask);
    // else
    //    CPU_SET((cpu_id + max_threads/2)% max_threads, &mask);

    // if (cpu_id % 2 == 0)                                             // FULL HT
    //    CPU_SET(cpu_id/2, &mask);
    // else
    //    CPU_SET((cpu_id/2) + (max_threads/2), &mask);

    // CPU_SET((cpu_id%4)*10 + (cpu_id%40)/4 + (cpu_id/40)*40, &mask);     // SOCKETS PINNING - Vader

    int ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");
}

long int find_total_nodes(isax_node *root_node)
{
    long int c = 1;
    if (root_node == NULL)
    {
        return 0;
    }
    else
    {
        c += find_total_nodes(root_node->left_child);
        c += find_total_nodes(root_node->right_child);
        return c;
    }
}

long int find_total_leafs_nodes(isax_node *root_node)
{

    if (root_node == NULL)
    {
        return 0;
    }

    if (root_node->left_child == NULL && root_node->right_child == NULL)
        return 1;
    else
        return find_total_leafs_nodes(root_node->left_child) + find_total_leafs_nodes(root_node->right_child);
}

long int find_tree_height(isax_node *root_node)
{

    if (root_node == NULL)
    {
        return -1;
    }
    else
    {
        long int left_depth = find_tree_height(root_node->left_child);
        long int right_depth = find_tree_height(root_node->right_child);

        if (left_depth > right_depth)
            return left_depth + 1;
        else
            return right_depth + 1;
    }
}

long int find_total_tree_leafs_depths(isax_node *root_node, long int depth)
{

    static long int total_leafs_depths = 0;
    if (depth == 0)
    { // if we change tree, initialize again the variable
        total_leafs_depths = 0;
    }

    if (root_node == NULL)
    {
        return 0;
    }
    else if (root_node->left_child == NULL && root_node->right_child == NULL)
    { // is_leaf
        total_leafs_depths += depth;
    }

    // NOT TRUE: intermediate node
    find_total_tree_leafs_depths(root_node->left_child, depth + 1);
    find_total_tree_leafs_depths(root_node->right_child, depth + 1);

    return total_leafs_depths;
}

void get_tree_statistics_manol(isax_index *index)
{
    long int empty_subtrees_buffers = 0;
    non_empty_subtrees_cnt = 0;

    long int total_nodes = 0;
    long int tree_height = 0;
    long int total_tree_leafs_depths = 0;

    long int total_leafs_nodes = 0;

    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            empty_subtrees_buffers++;
            continue;
        }

        non_empty_subtrees_cnt++;

        total_nodes += find_total_nodes((isax_node *)current_fbl_node->node);
        long int subtree_height = find_tree_height((isax_node *)current_fbl_node->node);
        tree_height += subtree_height;

        total_tree_leafs_depths += find_total_tree_leafs_depths((isax_node *)current_fbl_node->node, 0);

        total_leafs_nodes += find_total_leafs_nodes((isax_node *)current_fbl_node->node);
    }

    /*printf("[MC] -- Tree stats...\n");
    printf("Total buffers(2^16): %d\n", index->fbl->number_of_buffers);
    printf("Total tree nodes: %ld\n", total_nodes);
    printf("Total leafs nodes: %ld\n", total_leafs_nodes);
    printf("Total tree height: %ld\n", tree_height);
    printf("Total tree leafs depth: %ld\n", total_tree_leafs_depths);
    printf("Average leaf depth: %Lf\n", (long double)total_tree_leafs_depths / total_leafs_nodes);
    printf("Empty subtrees: %ld\n", empty_subtrees_buffers);
    printf("Non Empty subtrees: %ld\n", non_empty_subtrees_cnt);
    printf("--------------------------------------\n\n");*/
}

inline void get_tree_statistics_manol__2(isax_index *index)
{

    /*long int empty_subtrees_buffers = 0;
    non_empty_subtrees_cnt = 0;

    long int total_nodes = 0;
    long int tree_height = 0;
    long int total_tree_leafs_depths = 0;

    long int total_leafs_nodes = 0;



    char str[80];
    sprintf(str, "tree_statistics/%d.txt", my_rank);
    FILE *fp;
    fp  = fopen (str, "a");
    fprintf(fp, "Total buffers(2^16): %d\n", index->fbl->number_of_buffers);


    node_list nodelist;
    nodelist.nlist = malloc(sizeof(isax_node*)*pow(2, index->settings->paa_segments));
    nodelist.node_amount = 0;

    // maintain root nodes into an array, so that it is possible to execute FAI
    for (int j = 0; j < index->fbl->number_of_buffers; j++ ) {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized) {
            continue;
        }

        nodelist.nlist[nodelist.node_amount] = current_fbl_node->node;
        if (!current_fbl_node->node) {
            printf ("Error: node is NULL!!!!\t"); fflush(stdout);
            getchar();
        }
        nodelist.node_amount++;
    }

    int start = my_rank * (nodelist.node_amount / comm_sz);
    int end = (my_rank + 1)*nodelist.node_amount / comm_sz;
    empty_subtrees_buffers = -1;
    for (int i=start; i <= end; i++) {
        isax_node *sub_root = nodelist.nlist[i];

        non_empty_subtrees_cnt++;

        total_nodes += find_total_nodes(sub_root);
        long int subtree_height = find_tree_height(sub_root);
        tree_height += subtree_height;


        fprintf(fp, "%ld ", subtree_height);

        total_tree_leafs_depths += find_total_tree_leafs_depths(sub_root, 0);

        total_leafs_nodes += find_total_leafs_nodes(sub_root);
    }

    fprintf(fp, "\n");
    fprintf(fp, "Total tree nodes: %ld\n", total_nodes);
    fprintf(fp, "Total leafs nodes: %ld\n", total_leafs_nodes);

    fprintf(fp, "Total tree height: %ld\n", tree_height);
    fprintf(fp, "Total tree leafs depth: %ld\n", total_tree_leafs_depths);
    fprintf(fp, "Average leaf depth: %Lf\n", (long double)total_tree_leafs_depths/total_leafs_nodes);


    fprintf(fp, "Empty subtrees: %ld\n", empty_subtrees_buffers);
    fprintf(fp, "Non Empty subtrees: %ld\n", non_empty_subtrees_cnt);
    fprintf(fp, "--------------------------------------\n\n");

    fclose(fp);*/
}

long int count_ts_in_nodes(isax_node *root_node, const char parallelism_in_subtree, const char recBuf_helpers_exist)
{
    long int my_subtree_nodes = 0;

    if (!root_node->is_leaf)
    {
        my_subtree_nodes = count_ts_in_nodes(root_node->left_child, parallelism_in_subtree, recBuf_helpers_exist);
        my_subtree_nodes += count_ts_in_nodes(root_node->right_child, parallelism_in_subtree, recBuf_helpers_exist);
        return my_subtree_nodes;
    }
    else
    { // EDO: leaf
        // EDO: return my height
        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && recBuf_helpers_exist) ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && root_node->recBuf_leaf_helpers_exist))
        {
            if (root_node->fai_leaf_size == 0)
            {
                return root_node->leaf_size;
            }
            else if (root_node->fai_leaf_size < root_node->leaf_size)
            {
                printf("root_node->fai_leaf_size < root_node->leaf_size  !!!!\n");
                fflush(stdout);
            }
            return root_node->fai_leaf_size;
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW)
        {
            return root_node->buffer->partial_buffer_size;
        }
        else
        {
            return root_node->leaf_size;
        }
    }
}

inline void check_validity(isax_index *index, long int ts_num)
{

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }
    // 26/03/2021 LEFT->MANOL
    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            // EDO: COUNTER empty subtrees
            continue;
        }

        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
        }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, NO_PARALLELISM_IN_SUBTREE, 0);
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees)
        {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees)
        {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10)
        {
            cnt_1_10++;
        }
        else if (tmp_num >= 10 && tmp_num < 100)
        {
            cnt_10_100++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000)
        {
            cnt_100_1000++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000)
        {
            cnt_1000_10000++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000)
        {
            cnt_10000_100000++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000)
        {
            cnt_100000_1000000++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000)
        {
            cnt_1000000_10000000++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);
}

inline void check_validity_ekosmas(isax_index *index, long int ts_num)
{

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
        }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, NO_PARALLELISM_IN_SUBTREE, 0);
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees)
        {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees)
        {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10)
        {
            cnt_1_10++;
        }
        else if (tmp_num >= 10 && tmp_num < 100)
        {
            cnt_10_100++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000)
        {
            cnt_100_1000++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000)
        {
            cnt_1000_10000++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000)
        {
            cnt_10000_100000++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000)
        {
            cnt_100000_1000000++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000)
        {
            cnt_1000000_10000000++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);
}

float *index_creation_pRecBuf_new_ekosmas_func(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree, int create_index_flag)
{
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series // expected_records

    file_position_type total_records_per_process = total_records; // manolis
    // if each node has the whole dataset into its disk, must calculate and seek into the appropriate file position
    if (!mpi_already_splitted_dataset &&
        comm_sz > 1 &&
        /*[MC Addition]*/
        /*query_mode != DISTRIBUTED_QUERY_ANSWERING_STATIC &&
        query_mode != DISTRIBUTED_QUERY_ANSWERING_DYNAMIC &&
        query_mode != DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR*/
        !IS_RUNNING_DISTRIBUTED_QUERIES)
    {
        total_records_per_process = total_records / comm_sz; // if we have odd number of node, the last node will get one time series extra
    }

    fseek(ifile, 0L, SEEK_SET);

    if (!IS_EVEN(comm_sz) &&
        IS_LAST_NODE(my_rank, comm_sz) &&
        /*[MC Addition]*/
        /*query_mode != DISTRIBUTED_QUERY_ANSWERING_STATIC &&
        query_mode != DISTRIBUTED_QUERY_ANSWERING_DYNAMIC &&
        query_mode != DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR*/
        !IS_RUNNING_DISTRIBUTED_QUERIES)
    { // odd number of nodes. So the last node on ranks will process one time series more
        total_records_per_process++;
    }

    if (total_records_per_process < ts_num)
    {
        fprintf(stderr, "[MC] - Index creation: File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "raw_file" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    float *rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    //! node_list *rawdatanodelist = preprocessing_botao_to_manos(dataset, dataset_size, idx_1, my_rank%total_nodes_of_group , groupnodenumber, 4000, 1.05f); //! 
    //!rawfile = rawdatanodelist->rawfile;
    COUNT_INPUT_TIME_START

    long long int possition_to_file = 0;
    possition_to_file = ts_num * (sizeof(float) * time_series_size) * file_part_to_load; // 1024 it is need it !not to be used with multinode

    if (!mpi_already_splitted_dataset &&
        comm_sz > 1 &&
        /*[MC Addition]*/
        /*query_mode != DISTRIBUTED_QUERY_ANSWERING_STATIC &&
        query_mode != DISTRIBUTED_QUERY_ANSWERING_DYNAMIC &&
        query_mode != DISTRIBUTED_QUERY_ANSWERING_DYNAMIC_COORDINATOR*/
        !IS_RUNNING_DISTRIBUTED_QUERIES) // for parallel queries, every node should create all index
    {
        int my_new_rank = my_rank;

        // for 2 nodes only
        if (create_index_flag == SIMPLE_WORK_STEALING_CREATE_OTHER_NODE_INDEX)
        {
            // i change rank to load also the other node dataset
            if (my_rank == 0)
            {
                my_new_rank = 1;
            }

            if (my_rank == 1)
            {
                my_new_rank = 0;
            }
        } // so now the node0 can read the file part that belongs to node1, and node1 can read the file part that belongs to node0
        else if (create_index_flag == SIMPLE_WORK_STEALING_CREATE_OTHER_NODE_INDEX_MULTINODE_PAIR)
        {
            if (my_rank % 2 == 0)
            {
                my_new_rank = my_rank + 1; // 0 is pairing with 1, 2 with 3 and so on...
            }
            else
            {
                my_new_rank = my_rank - 1;
            }
        }

        possition_to_file = my_new_rank * ts_num * (sizeof(float) * time_series_size);

        if (file_part_to_load != 0)
        {
            possition_to_file = ts_num * (sizeof(float) * time_series_size) * file_part_to_load; // 1024 it is need it !not to be used with multinode
        }

        if (!IS_EVEN(comm_sz) && IS_LAST_NODE(my_new_rank, comm_sz))
        {                                                                                        // odd number of nodes. So the last node on ranks will process one time series more
            possition_to_file = my_new_rank * (ts_num - 1) * (sizeof(float) * time_series_size); // else it will not take the one extra times series that is need it to process
            if (file_part_to_load != 0)
            {
                possition_to_file = (ts_num - 1) * (sizeof(float) * time_series_size) * file_part_to_load;
            }
        }
    }

    int returned_val = fseek(ifile, possition_to_file, SEEK_SET); // manolis
    if (returned_val != 0)
    {
        printf("Error on fseek()\n");
        exit(1);
    }

    size_t read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile); // int -> size_t //!
    
    //!read_number = rawdatanodelist->data_amount;
    //!ts_num =  rawdatanodelist->data_amount;

    // manolis
    if ((read_number / time_series_size) != ts_num)
    { // time_series_size := 128 or 256
        printf("Must read: %ld but it reads %ld times series\n", ts_num, read_number / time_series_size);
        printf("Read times series != than the times series to be read\n");
        exit(1);
    }
    COUNT_INPUT_TIME_END
    // printf("[MC] - DEBUG - INPUT TIME END: After reading the dataset calculations: %f\n", total_input_time);

    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Index Creation with %d threads.\n", maxreadthread);
    }

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas(index->settings->initial_fbl_buffer_size, pow(2, index->settings->paa_segments), index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxreadthread];                                                                         //[MC] - Changed it to use maxReadThread
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxreadthread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num

    unsigned long next_block_to_process = 0;
    int node_counter = 0; // required for tree construction using fai

    volatile unsigned long *next_iSAX_group = calloc(index->fbl->max_total_size, sizeof(unsigned long)); // EKOSMAS ADDED - 02 NOVEMBER 2020

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    // pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxreadthread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < maxreadthread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        input_data[i].parallelism_in_subtree = parallelism_in_subtree;
        input_data[i].next_iSAX_group = next_iSAX_group; // EKOSMAS ADDED - 02 NOVEMBER 2020

        input_data[i].rawfile = rawfile;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    // printf("[MC]: Max Query Thread: %d\n", maxquerythread);
    for (int i = 0; i < maxreadthread; i++)
    {
        pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new_ekosmas, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxreadthread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity_ekosmas(index, ts_num);

    return rawfile;
}


float *index_creation_pRecBuf_new_ekosmas(const char *ifilename, long int ts_num, isax_index *index, int create_index_flag)
{
    float *raw_file = index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, NO_PARALLELISM_IN_SUBTREE, create_index_flag);

    COUNT_TOTAL_TIME_END

    if (all_nodes_index_all_given_dataset == 1)
    {
        get_tree_statistics_manol__2(index);
    }
    else
    {
        get_tree_statistics_manol(index);
    }

    COUNT_TOTAL_TIME_START

    return raw_file;
}

float *index_creation_pRecBuf_new_ekosmas_MESSI_with_enhanced_blocking_parallelism(const char *ifilename, long int ts_num, isax_index *index, int create_index_flag)
{
    return index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, BLOCKING_PARALLELISM_IN_SUBTREE, create_index_flag);
}

inline void tree_index_creation_from_pRecBuf_fai_blocking(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    int j;
    bool has_record;
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while (1)
    {

        j = __sync_fetch_and_add(input_data->node_counter, 1);
        if (j >= index->fbl->number_of_buffers)
        {
            break;
        }

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        for (int k = 0; k < maxquerythread; k++)
        {

            for (int i = 0; i < current_fbl_node->buffer_size[k]; i++)
            {
                r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
                r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;
                // Add record to index
                add_record_to_node_inmemory(index, (isax_node *)current_fbl_node->node, r, 1);
            }
        }
    }

    free(r);
}

inline unsigned long populate_tree_with_locks_blocking_with_parallelism(buffer_data_inmemory_ekosmas *input_data, int j, parallel_fbl_soft_buffer_ekosmas *current_fbl_node, isax_node_record *r, unsigned long my_id)
{
    isax_index *index = input_data->index;

    isax_node *root_node = current_fbl_node->node;
    pthread_mutex_t *tmp_lock_node = NULL;

    // create and initialize a new lock_node
    if (!root_node->lock_node)
    {
        tmp_lock_node = malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(tmp_lock_node, NULL);
    }

    // try to establish new lock_node
    if (tmp_lock_node != NULL && (root_node->lock_node || !CASPTR(&root_node->lock_node, NULL, tmp_lock_node)))
    {
        // free memory
        pthread_mutex_destroy(tmp_lock_node);
    }

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;

    while (!current_fbl_node->finished)
    {
        iSAX_group = __sync_fetch_and_add(&(input_data->next_iSAX_group[j]), 1);

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num)
        {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (process_recBuf_id == maxquerythread)
        {
            break;
        }

        int k = process_recBuf_id;
        int i = iSAX_group - prev_recBuf_iSAX_num;

        r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
        r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_locks(index, root_node, r);

        subtree_nodes++;
    }

    if (!current_fbl_node->finished)
    {
        current_fbl_node->finished = 1;
    }

    return subtree_nodes;
}

inline void backoff_delay_lockfree_subtree_parallel_blocking_with_parallelism(unsigned long backoff, volatile int *stop)
{
    if (!backoff)
    {
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline unsigned long count_nodes_in_RecBuf_for_subtree_parallel_blocking_with_parallelism(parallel_fbl_soft_buffer_ekosmas *current_fbl_node, volatile int *stop)
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++)
    {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}

static inline void scan_for_unprocessed_RecBufs_blocking_with_parallelism(buffer_data_inmemory_ekosmas *input_data, isax_node_record *r, unsigned long my_id)
{

    isax_index *index = input_data->index;

    if (DO_NOT_HELP)
    {
        return;
    }

    unsigned long backoff_time = backoff_multiplier;

    if (my_num_subtree_construction)
    {
        backoff_time *= (unsigned long)BACKOFF_SUBTREE_DELAY_PER_NODE;
    }
    else
    {
        backoff_time = 0;
    }

    for (int i = 0; i < index->fbl->number_of_buffers && !all_RecBufs_processed; i++)
    {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized || current_fbl_node->finished)
        {
            continue;
        }

        unsigned long num_nodes = count_nodes_in_RecBuf_for_subtree_parallel_blocking_with_parallelism(current_fbl_node, &current_fbl_node->finished);
        backoff_delay_lockfree_subtree_parallel_blocking_with_parallelism(backoff_time * num_nodes, &current_fbl_node->finished);

        if (current_fbl_node->finished)
        {
            recBufs_helping_avoided_cnt++;
            continue;
        }

        recBufs_helped_cnt++;

        populate_tree_with_locks_blocking_with_parallelism(input_data, i, current_fbl_node, r, my_id);
    }

    if (!all_RecBufs_processed)
    {
        all_RecBufs_processed = 1;
    }

    if (recBufs_helping_avoided_cnt)
    {
        COUNT_SUBTREE_HELP_AVOIDED(recBufs_helping_avoided_cnt)
    }

    if (recBufs_helped_cnt)
    {
        COUNT_SUBTREES_HELPED(recBufs_helped_cnt)
    }
}

static inline void tree_index_creation_from_pRecBuf_fai_blocking_with_parallelism(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    int j;
    bool has_record;
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while (1)
    {
        j = __sync_fetch_and_add(input_data->node_counter, 1);
        if (j >= index->fbl->number_of_buffers)
        {
            break;
        }

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        COUNT_MY_TIME_START
        populate_tree_with_locks_blocking_with_parallelism(input_data, j, current_fbl_node, r, input_data->workernumber);
        COUNT_MY_TIME_FOR_SUBTREE_END
        my_num_subtree_construction++;
    }

    scan_for_unprocessed_RecBufs_blocking_with_parallelism(input_data, r, input_data->workernumber);

    free(r);
}

// void* index_creation_pRecBuf_worker_new_botao
void *index_creation_pRecBuf_worker_new(void *transferdata)
{

    // ADDED BY EKOSMAS - JUNE 02, 2020
    //      FROM HERE
    buffer_data_inmemory *input_data = (buffer_data_inmemory *)transferdata;
    threadPin(input_data->workernumber, maxquerythread);
    //      UP TO HERE

    sax_type *sax = malloc(sizeof(sax_type) * ((buffer_data_inmemory *)transferdata)->index->settings->paa_segments);

    // struct timeval workertimestart;
    // struct timeval writetiemstart;
    // struct timeval workercurenttime;
    // struct timeval writecurenttime;
    // double worker_total_time,tee,tss;
    // gettimeofday(&workertimestart, NULL);

    unsigned long roundfinishednumber;

    unsigned long start_number;
    unsigned long stop_number = ((buffer_data_inmemory *)transferdata)->stop_number;
    file_position_type *pos = malloc(sizeof(file_position_type));
    isax_index *index = ((buffer_data_inmemory *)transferdata)->index;
    ts_type *ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    int paa_segments = ((buffer_data_inmemory *)transferdata)->index->settings->paa_segments;

    unsigned long i = 0;
    float *raw_file = ((buffer_data_inmemory *)transferdata)->ts;
    while (1)
    {
        start_number = __sync_fetch_and_add(((buffer_data_inmemory *)transferdata)->shared_start_number, read_block_length);
        if (start_number > stop_number)
        {
            break;
        }
        else if (start_number > stop_number - read_block_length)
        {
            roundfinishednumber = stop_number;
        }
        else
        {
            roundfinishednumber = start_number + read_block_length;
        }
        for (i = start_number; i < roundfinishednumber; i++)
        {
            // EKOSMAS: why is this memcpy required?
            memcpy(ts, &(raw_file[i * index->settings->timeseries_size]), sizeof(float) * index->settings->timeseries_size);
            if (sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
                            index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                            index->settings->sax_bit_cardinality) == SUCCESS)
            {
                *pos = (file_position_type)(i * index->settings->timeseries_size);
                memcpy(&(index->sax_cache[i * index->settings->paa_segments]), sax, sizeof(sax_type) * index->settings->paa_segments);

                isax_pRecBuf_index_insert_inmemory(index, sax, pos, ((buffer_data_inmemory *)transferdata)->lock_firstnode, ((buffer_data_inmemory *)transferdata)->workernumber, ((buffer_data_inmemory *)transferdata)->total_workernumber);
            }
            else
            {
                fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
            }
        }
    }

    free(pos);
    free(sax);
    free(ts);
    // gettimeofday(&workercurenttime, NULL);
    // tss = workertimestart.tv_sec*1000000 + (workertimestart.tv_usec);
    // tee = workercurenttime.tv_sec*1000000  + (workercurenttime.tv_usec);
    // worker_total_time += (tee - tss);
    // printf("the worker time is %f\n",worker_total_time );

    pthread_barrier_wait(((buffer_data_inmemory *)transferdata)->lock_barrier1);
    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    // pthread_barrier_wait(((buffer_data_inmemory*)transferdata)->lock_barrier2);              // REMOVED BY EKOSMAS (15/06/2020)
    bool have_record = false;
    int j;
    isax_node_record *r = malloc(sizeof(isax_node_record));
    // int preworkernumber=((buffer_data_inmemory*)transferdata)->total_workernumber;

    // for (j=((trans_fbl_input*)input)->start_number; j<((trans_fbl_input*)input)->stop_number; j++)
    while (1)
    {

        j = __sync_fetch_and_add(((buffer_data_inmemory *)transferdata)->node_counter, 1);

        if (j >= index->fbl->number_of_buffers)
        {
            break;
        }
        // fbl_soft_buffer *current_fbl_node = &index->fbl->soft_buffers[j];
        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        int i;
        have_record = false;
        for (int k = 0; k < ((buffer_data_inmemory *)transferdata)->total_workernumber; k++)
        {
            if (current_fbl_node->buffer_size[k] > 0)
                have_record = true;
            for (i = 0; i < current_fbl_node->buffer_size[k]; i++)
            {
                r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
                r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;
                // Add record to index
                // printf("the position 1 is %d\n",*(r->position));
                // sleep(1);
                add_record_to_node_inmemory(index, (isax_node *)current_fbl_node->node, r, 1); // EKOSMAS: CHANGED 03 SEPTEMBER 2020
            }
        }
        if (have_record)
        {
            flush_subtree_leaf_buffers_inmemory(index, (isax_node *)current_fbl_node->node);

            // clear FBL records moved in LBL buffers

            // clear records read from files (free only prev sax buffers)
        }
    }
    free(r);
}

void *index_creation_pRecBuf_worker_new_ekosmas(void *transferdata)
{
    // printf("2. index_creation_pRecBuf_worker_new_ekosmas\n");
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    // threadPin(input_data->workernumber, maxquerythread);
    threadPin(input_data->workernumber, maxreadthread);

    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    float *rawfile = input_data->rawfile;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;

    file_position_type pos;
    sax_type *sax = malloc(sax_byte_size); // CHANGED BY EKOSMAS - 11/05/2020

    unsigned long i, block_num, my_ts_start, my_ts_end;
    while (1)
    {
        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if (block_num > total_blocks)
        {
            break;
        }

        my_ts_start = block_num * read_block_length;
        if (block_num == total_blocks)
        { // there may still remain some more data series (i.e. less than #read_block_length)
            my_ts_end = ts_num;
        }
        else
        {
            my_ts_end = (block_num + 1) * read_block_length;
        }

        for (i = my_ts_start; i < my_ts_end; i++)
        {
            // EKOSMAS: why is this memcpy required?
            // EKOSMAS: TODO: check if these memcpys result in better performance
            // memcpy(ts,&(rawfile[i*index->settings->timeseries_size]), sizeof(float)*index->settings->timeseries_size);
            // if(sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
            if (sax_from_ts(
                    (ts_type *)&rawfile[i * index->settings->timeseries_size], // CHANGED BY EKOSMAS - 11/05/2020
                    sax,
                    index->settings->ts_values_per_paa_segment,
                    paa_segments,
                    index->settings->sax_alphabet_cardinality,
                    index->settings->sax_bit_cardinality) == SUCCESS)
            {
                pos = (file_position_type)(i * index->settings->timeseries_size);
                // memcpy(&(index->sax_cache[i*sax_byte_size]), sax, sax_byte_size);                    // REMOVED BY EKOSMAS - 04/06/2020 // CHANGED BY EKOSMAS - 11/05/2020

                isax_pRecBuf_index_insert_inmemory_ekosmas(
                    index,
                    sax,
                    &pos,                       // CHANGED BY EKOSMAS - 04/06/2020
                    input_data->lock_firstnode, // CHANGED BY EKOSMAS - 11/05/2020
                    input_data->workernumber,   // CHANGED BY EKOSMAS - 11/05/2020
                    maxquerythread);
            }
            else
            {
                fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
            }
        }
    }

    // free(pos);
    free(sax);
    // free(ts);

    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    // 26/03/2021 LEFT->MANOL

    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    if (input_data->parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE)
    {
        // manol
        // printf("EDO!!!\n");
        tree_index_creation_from_pRecBuf_fai_blocking(transferdata); // 26/03/2021 LEFT->MANOL
    }
    else
    { // transferdata->parallelism_in_subtree == BLOCKING_PARALLELISM_IN_SUBTREE
        // printf("H edo ????\n");
        tree_index_creation_from_pRecBuf_fai_blocking_with_parallelism(transferdata);
    }
}

root_mask_type isax_pRecBuf_index_insert_inmemory(isax_index *index,
                                                  sax_type *sax,
                                                  file_position_type *pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber)
{
    int i, t;
    int totalsize = index->settings->max_total_buffer_size;

    // Create mask for the first bit of the sax representation

    // Step 1: Check if there is a root node that represents the
    //         current node's sax representation

    // TODO: Create INSERTION SHORT AND BINARY SEARCH METHODS.

    root_mask_type first_bit_mask = 0x00;

    CREATE_MASK(first_bit_mask, index, sax);

    insert_to_pRecBuf(
        (parallel_first_buffer_layer *)(index->fbl),
        sax,
        pos,
        first_bit_mask,
        index,
        lock_firstnode,
        workernumber,
        total_workernumber);

    return first_bit_mask;
}
root_mask_type isax_pRecBuf_index_insert_inmemory_ekosmas(isax_index *index,
                                                          sax_type *sax,
                                                          file_position_type *pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber)
{
    int i, t;
    int totalsize = index->settings->max_total_buffer_size;

    // Create mask for the first bit of the sax representation

    // Step 1: Check if there is a root node that represents the
    //         current node's sax representation

    // TODO: Create INSERTION SHORT AND BINARY SEARCH METHODS.

    root_mask_type first_bit_mask = 0x00;

    CREATE_MASK(first_bit_mask, index, sax);

    insert_to_pRecBuf_ekosmas(
        (parallel_first_buffer_layer_ekosmas *)(index->fbl),
        sax,
        pos,
        first_bit_mask,
        index,
        lock_firstnode,
        workernumber,
        total_workernumber);

    return first_bit_mask;
}

enum response flush_subtree_leaf_buffers_inmemory(isax_index *index, isax_node *node)
{

    if (node->is_leaf && node->filename != NULL)
    {
        // Set that unloaded data exist in disk
        if (node->buffer->partial_buffer_size > 0 || node->buffer->tmp_partial_buffer_size > 0)
        {
            node->has_partial_data_file = 1;
        }
        // Set that the node has flushed full data in the disk
        if (node->buffer->full_buffer_size > 0 || node->buffer->tmp_full_buffer_size > 0)
        {
            node->has_full_data_file = 1;
        }

        if (node->has_full_data_file)
        {
            int prev_rec_count = node->leaf_size - (node->buffer->full_buffer_size + node->buffer->tmp_full_buffer_size);

            int previous_page_size = ceil((float)(prev_rec_count * index->settings->full_record_size) / (float)PAGE_SIZE);
            int current_page_size = ceil((float)(node->leaf_size * index->settings->full_record_size) / (float)PAGE_SIZE);
            __sync_fetch_and_add(&(index->memory_info.disk_data_full), (current_page_size - previous_page_size));
            // index->memory_info.disk_data_full += (current_page_size - previous_page_size);
        }
        if (node->has_partial_data_file)
        {
            int prev_rec_count = node->leaf_size - (node->buffer->partial_buffer_size + node->buffer->tmp_partial_buffer_size);

            int previous_page_size = ceil((float)(prev_rec_count * index->settings->partial_record_size) / (float)PAGE_SIZE);
            int current_page_size = ceil((float)(node->leaf_size * index->settings->partial_record_size) / (float)PAGE_SIZE);

            // index->memory_info.disk_data_partial += (current_page_size - previous_page_size);
            __sync_fetch_and_add(&(index->memory_info.disk_data_partial), (current_page_size - previous_page_size));
        }
        if (node->has_full_data_file && node->has_partial_data_file)
        {
            printf("WARNING: (Mem size counting) this leaf has both partial and full data.\n");
        }
        // index->memory_info.disk_data_full += (node->buffer->full_buffer_size +
        // node->buffer->tmp_full_buffer_size);
        __sync_fetch_and_add(&(index->memory_info.disk_data_full), (node->buffer->full_buffer_size + node->buffer->tmp_full_buffer_size));
        // index->memory_info.disk_data_partial += (node->buffer->partial_buffer_size +
        // node->buffer->tmp_partial_buffer_size);
        __sync_fetch_and_add(&(index->memory_info.disk_data_partial), (node->buffer->partial_buffer_size + node->buffer->tmp_partial_buffer_size));
        // flush_node_buffer(node->buffer, index->settings->paa_segments,
        // index->settings->timeseries_size,
        // node->filename);
    }
    else if (!node->is_leaf)
    {
        flush_subtree_leaf_buffers_inmemory(index, node->left_child);
        flush_subtree_leaf_buffers_inmemory(index, node->right_child);
    }

    return SUCCESS;
}

/* ---------- Chatzakis Code ---------- */

/**
 * @brief
 *
 * @param query_index
 */
void find_query_index_leafs_chatzakis(isax_index *query_index)
{
    int all_buffers = query_index->fbl->number_of_buffers;

    for (int i = 0; i < all_buffers; i++)
    {
        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(query_index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }
    }
}

void get_tree_statistics_chatz(isax_index *index)
{
    long int empty_subtrees_buffers = 0;
    non_empty_subtrees_cnt = 0;

    long int total_nodes = 0;
    long int tree_height = 0;
    long int total_tree_leafs_depths = 0;

    long int total_leafs_nodes = 0;

    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            empty_subtrees_buffers++;
            continue;
        }

        non_empty_subtrees_cnt++;

        total_nodes += find_total_nodes((isax_node *)current_fbl_node->node);
        long int subtree_height = find_tree_height((isax_node *)current_fbl_node->node);
        tree_height += subtree_height;

        total_tree_leafs_depths += find_total_tree_leafs_depths((isax_node *)current_fbl_node->node, 0);

        total_leafs_nodes += find_total_leafs_nodes((isax_node *)current_fbl_node->node);
    }

    printf("[MC] -- Tree stats...\n");
    printf("Total buffers(2^16): %d\n", index->fbl->number_of_buffers);
    printf("Total tree nodes: %ld\n", total_nodes);
    printf("Total leafs nodes: %ld\n", total_leafs_nodes);
    printf("Total tree height: %ld\n", tree_height);
    printf("Total tree leafs depth: %ld\n", total_tree_leafs_depths);
    printf("Average leaf depth: %Lf\n", (long double)total_tree_leafs_depths / total_leafs_nodes);
    printf("Empty subtrees: %ld\n", empty_subtrees_buffers);
    printf("Non Empty subtrees: %ld\n", non_empty_subtrees_cnt);
    printf("--------------------------------------\n\n");
}

/**
 * @brief Splits the data into chunks, each node group gets assigned a chunk and builds the index.
 * 
 * @param ifilename Dataset path
 * @param ts_num Total data seties
 * @param index iSAX index parameter
 * @param parallelism_in_subtree Flag for index creation mode
 * @param node_groups Node groups array
 * @param total_node_groups Total number of node groups
 * @param total_nodes_per_nodegroup Total number of nodes in each group
 * @return float* Raw float array
 */
float *index_creation_node_groups_chatzakis(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree, pdr_node_group *node_groups, int total_node_groups, int total_nodes_per_nodegroup)
{
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series // expected_records

    file_position_type total_records_per_group = total_records; // manolis
    // if each node has the whole dataset into its disk, must calculate and seek into the appropriate file position
    if (!mpi_already_splitted_dataset && total_node_groups > 1)
    {
        total_records_per_group = total_records / total_node_groups; // if we have odd number of node, the last node will get one time series extra
    }

    fseek(ifile, 0L, SEEK_SET);

    if (!IS_EVEN(total_node_groups) && (FIND_NODE_GROUP(my_rank, total_nodes_per_nodegroup) == total_node_groups - 1))
    { // odd number of nodes. So the last node on ranks will process one time series more
        total_records_per_group++;
    }

    if (total_records_per_group < ts_num)
    {
        fprintf(stderr, "[MC] - Index creation: File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "raw_file" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    float *rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START

    long long int possition_to_file = 0;
    possition_to_file = ts_num * (sizeof(float) * time_series_size) * file_part_to_load; // 1024 it is need it !not to be used with multinode

    if (!mpi_already_splitted_dataset && comm_sz > 1) // for parallel queries, every node should create all index
    {
        int my_new_rank = my_rank;

        possition_to_file = FIND_NODE_GROUP(my_new_rank, total_nodes_per_nodegroup) * ts_num * (sizeof(float) * time_series_size);

        if (!IS_EVEN(comm_sz) && IS_LAST_NODE(my_new_rank, comm_sz))
        {                                                                                                                                    // odd number of nodes. So the last node on ranks will process one time series more
            possition_to_file = FIND_NODE_GROUP(my_new_rank, total_nodes_per_nodegroup) * (ts_num - 1) * (sizeof(float) * time_series_size); // else it will not take the one extra times series that is need it to process
        }
    }

    int returned_val = fseek(ifile, possition_to_file, SEEK_SET); // manolis
    if (returned_val != 0)
    {
        printf("Error on fseek()\n");
        exit(1);
    }

    size_t read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile); // int -> size_t

    // manolis
    if ((read_number / time_series_size) != ts_num)
    { // time_series_size := 128 or 256
        printf("Must read: %ld but it reads %ld times series\n", ts_num, read_number / time_series_size);
        printf("Read times series != than the times series to be read\n");
        exit(-1);
    }
    COUNT_INPUT_TIME_END
    // printf("[MC] - DEBUG - INPUT TIME END: After reading the dataset calculations: %f\n", total_input_time);

    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Index Creation with %d threads.\n", maxreadthread);
    }

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas(index->settings->initial_fbl_buffer_size, pow(2, index->settings->paa_segments), index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxreadthread];                                                                         //[MC] - Changed it to use maxReadThread
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxreadthread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num

    unsigned long next_block_to_process = 0;
    int node_counter = 0; // required for tree construction using fai

    volatile unsigned long *next_iSAX_group = calloc(index->fbl->max_total_size, sizeof(unsigned long)); // EKOSMAS ADDED - 02 NOVEMBER 2020

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    // pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxreadthread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < maxreadthread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        input_data[i].parallelism_in_subtree = parallelism_in_subtree;
        input_data[i].next_iSAX_group = next_iSAX_group; // EKOSMAS ADDED - 02 NOVEMBER 2020

        input_data[i].rawfile = rawfile;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    // printf("[MC]: Max Query Thread: %d\n", maxquerythread);
    for (int i = 0; i < maxreadthread; i++)
    {
        pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new_ekosmas, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxreadthread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity_ekosmas(index, ts_num);

    return rawfile;
}

/*Botao to Manos: start copy*/

static unsigned int GraytoDecimal(unsigned int x)
{
    unsigned int y = x;
    while (x >>= 1)
        y ^= x;
    return y;
}

static unsigned int DecimaltoGray(unsigned int x)
{
    return x ^ (x >> 1);
}

void countmemory(parallel_first_buffer_layer *current_fbl, int *counterofmemory)
{
    for (int i = 0; i < current_fbl->total_worker_number; i++)
    {
        counterofmemory[i] = 0;
    }
    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        parallel_fbl_soft_buffer *current_buffer = &(current_fbl->soft_buffers[i]);
        if (current_buffer->initialized)
        {

            for (int j = 0; j < current_fbl->total_worker_number; j++)
            {
                counterofmemory[j] += current_buffer->buffer_size[j];
            }
        }
    }
}

void distribute_node_data(parallel_fbl_soft_buffer *current_buffer, int come_sz)
{
    file_position_type *tmp_pos_buffer;
    int tmp_buffer_size = 0;
    int *tmp_buffer_all = malloc(sizeof(int) * come_sz);
    for (int i = 0; i < come_sz; i++)
    {
        tmp_buffer_all[i] = 0;
        if (current_buffer->buffer_size[i] != 0)
        {
            tmp_pos_buffer = current_buffer->pos_records[i];
            tmp_buffer_size = current_buffer->buffer_size[i];
        }
    }

    for (int i = 0; i < tmp_buffer_size; i++)
    {
        tmp_buffer_all[i % comm_sz]++;
    }

    for (int i = 0; i < come_sz; i++)
    {
        current_buffer->buffer_size[i] = tmp_buffer_all[i];
    }
    for (int i = 0; i < come_sz; i++)
    {
        current_buffer->pos_records[i] = malloc(sizeof(file_position_type) * current_buffer->buffer_size[i]);
    }
    file_position_type *currentdir;
    currentdir = tmp_pos_buffer;
    for (int i = 0; i < come_sz; i++)
    {
        memcpy(current_buffer->pos_records[i], currentdir, sizeof(file_position_type) * current_buffer->buffer_size[i]);
        currentdir = &currentdir[current_buffer->buffer_size[i]];
    }
    current_buffer->max_buffer_size[0] = 1;
    free(tmp_pos_buffer);
    free(tmp_buffer_all);
}

void distribute_node_data_2(parallel_fbl_soft_buffer *current_buffer, int come_sz)
{
    file_position_type *tmp_pos_buffer;
    int tmp_buffer_size = 0;
    int *tmp_buffer_all = malloc(sizeof(int) * come_sz);
    for (int i = 0; i < come_sz; i++)
    {
        tmp_buffer_all[i] = 0;
        if (current_buffer->buffer_size[i] != 0)
        {
            tmp_pos_buffer = current_buffer->pos_records[i];
            tmp_buffer_size = current_buffer->buffer_size[i];
        }
    }

    for (int i = 0; i < tmp_buffer_size; i++)
    {
        tmp_buffer_all[i % comm_sz]++;
    }

    for (int i = 0; i < come_sz; i++)
    {
        current_buffer->buffer_size[i] = tmp_buffer_all[i];
    }
    for (int i = 0; i < come_sz; i++)
    {
        current_buffer->pos_records[i] = malloc(sizeof(file_position_type) * current_buffer->buffer_size[i]);
    }
    file_position_type *currentdir;
    currentdir = tmp_pos_buffer;
    for (int i = 0; i < come_sz; i++)
    {
        memcpy(current_buffer->pos_records[i], currentdir, sizeof(file_position_type) * current_buffer->buffer_size[i]);
        currentdir = &currentdir[current_buffer->buffer_size[i]];
    }
    current_buffer->max_buffer_size[0] = 1;
    free(tmp_pos_buffer);
    free(tmp_buffer_all);
}

void distribute_max_node_uni(parallel_first_buffer_layer *current_fbl, int roundnumber)
{

    int nodecounter = 0;
    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        if (current_fbl->soft_buffers[i].initialized)
        {
            nodecounter++;
        }
    }
    int *nodeamount = malloc(sizeof(int) * nodecounter);
    nodecounter = 0;
    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        if (current_fbl->soft_buffers[i].initialized)
        {
            nodeamount[nodecounter] = i;
            nodecounter++;
        }
    }

    for (int i = 0; i < nodecounter; i++)
    {
        int tmpbuffersizei = 0;
        for (int k = 0; k < current_fbl->total_worker_number; k++)
        {

            if (current_fbl->soft_buffers[nodeamount[i]].buffer_size[k] != 0)
                tmpbuffersizei = current_fbl->soft_buffers[nodeamount[i]].buffer_size[k];
        }

        for (int j = i + 1; j < nodecounter; j++)
        {
            int tmpbuffersizej = 0;
            for (int k = 0; k < current_fbl->total_worker_number; k++)
            {
                if (current_fbl->soft_buffers[nodeamount[j]].buffer_size[k] != 0)
                    tmpbuffersizej = current_fbl->soft_buffers[nodeamount[j]].buffer_size[k];
            }
            if (tmpbuffersizei < tmpbuffersizej)
            {
                int tmp = nodeamount[i];
                nodeamount[i] = nodeamount[j];
                nodeamount[j] = tmp;
            }
        }
    }
    for (int i = 0; i < roundnumber; i++)
    {
        printf("this is the %d node !!!!!!!!\n", nodeamount[i]);
        distribute_node_data(&(current_fbl->soft_buffers[nodeamount[i]]), current_fbl->total_worker_number);
    }
}

void distribute_max_node(parallel_first_buffer_layer *current_fbl, int nodenumber)
{
    if (nodenumber >= current_fbl->total_worker_number)
        exit(-1);

    int nodecounter = 0;
    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        if (current_fbl->soft_buffers[i].initialized)
        {
            if (current_fbl->soft_buffers[i].buffer_size[nodenumber] != 0)
            {
                nodecounter++;
            }
        }
    }
    int *nodeamount = malloc(sizeof(int) * nodecounter);
    nodecounter = 0;
    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        if (current_fbl->soft_buffers[i].initialized)
        {
            if (current_fbl->soft_buffers[i].buffer_size[nodenumber] != 0)
            {
                nodeamount[nodecounter] = i;
                nodecounter++;
            }
        }
    }
    for (int i = 0; i < nodecounter; i++)
    {
        for (int j = i + 1; j < nodecounter; j++)
        {
            if (current_fbl->soft_buffers[nodeamount[i]].buffer_size[nodenumber] < current_fbl->soft_buffers[nodeamount[j]].buffer_size[nodenumber])
            {
                int tmp = nodeamount[i];
                nodeamount[i] = nodeamount[j];
                nodeamount[j] = tmp;
            }
        }
    }
    for (int i = 0; i < nodecounter; i++)
    {
        int buffercounter = 0;
        for (int j = 0; j < current_fbl->total_worker_number; j++)
        {
            if (current_fbl->soft_buffers[nodeamount[i]].buffer_size[j] != 0)
                buffercounter++;
        }
        if (buffercounter == 1)
        {
            distribute_node_data(&(current_fbl->soft_buffers[nodeamount[i]]), current_fbl->total_worker_number);
            break;
        }
    }
}

void datadistribute(parallel_first_buffer_layer *current_fbl, int *counterofmemory)
{
    int maxnode, minnode, meanvalue;
    maxnode = 0;
    minnode = 0;
    meanvalue = 0;

    for (int kone = 0; kone < 0; kone++)
    {
        for (int i = 0; i < current_fbl->total_worker_number; i++)
        {
            distribute_max_node(current_fbl, i);
            countmemory(current_fbl, counterofmemory);
        }
    }

    for (int i = 0; i < current_fbl->total_worker_number; i++)
    {
        if (counterofmemory[maxnode] < counterofmemory[i])
            maxnode = i;
        if (counterofmemory[minnode] > counterofmemory[i])
            minnode = i;
        meanvalue += counterofmemory[i] / current_fbl->total_worker_number;
    }
    float k;
    while (k = (float)counterofmemory[maxnode] / meanvalue > 1.05f)
    {
        distribute_max_node(current_fbl, maxnode);
        countmemory(current_fbl, counterofmemory);
        for (int i = 0; i < current_fbl->total_worker_number; i++)
        {
            if (counterofmemory[maxnode] < counterofmemory[i])
                maxnode = i;
            if (counterofmemory[minnode] > counterofmemory[i])
                minnode = i;
        }
    }
}

void data_distribute_input(parallel_first_buffer_layer *current_fbl, int *counterofmemory, int round, float brate)
{
    int maxnode, minnode, meanvalue;
    maxnode = 0;
    minnode = 0;
    meanvalue = 0;
    for (int kone = 0; kone < round; kone++)
    {
        for (int i = 0; i < current_fbl->total_worker_number; i++)
        {
            distribute_max_node(current_fbl, i);
            countmemory(current_fbl, counterofmemory);
        }
    }
    // distribute_max_node_uni(current_fbl,round);
    // countmemory(current_fbl,counterofmemory);

    for (int i = 0; i < current_fbl->total_worker_number; i++)
    {
        if (counterofmemory[maxnode] < counterofmemory[i])
            maxnode = i;
        if (counterofmemory[minnode] > counterofmemory[i])
            minnode = i;
        meanvalue += counterofmemory[i] / current_fbl->total_worker_number;
    }
    float k;
    while (k = (float)counterofmemory[maxnode] / meanvalue > brate)
    {
        distribute_max_node(current_fbl, maxnode);
        countmemory(current_fbl, counterofmemory);
        for (int i = 0; i < current_fbl->total_worker_number; i++)
        {
            if (counterofmemory[maxnode] < counterofmemory[i])
                maxnode = i;
            if (counterofmemory[minnode] > counterofmemory[i])
                minnode = i;
        }
    }
}

void data_distribute_input_2(parallel_first_buffer_layer *current_fbl, int *counterofmemory, int round, float brate)
{
    int maxnode, minnode, meanvalue;
    maxnode = 0;
    minnode = 0;
    meanvalue = 0;
    // for (int kone = 0; kone < round; kone++)
    {
        // for (int i = 0; i < current_fbl->total_worker_number; i++)
        {
            // distribute_max_node(current_fbl,i);
            // countmemory(current_fbl,counterofmemory);
        }
    }
    // distribute_max_node_uni(current_fbl,round);
    // countmemory(current_fbl,counterofmemory);

    for (int i = 0; i < current_fbl->total_worker_number; i++)
    {
        if (counterofmemory[maxnode] < counterofmemory[i])
            maxnode = i;
        if (counterofmemory[minnode] > counterofmemory[i])
            minnode = i;
        meanvalue += counterofmemory[i] / current_fbl->total_worker_number;
    }
    float k;
    while (k = (float)counterofmemory[maxnode] / meanvalue > brate)
    {
        distribute_max_node(current_fbl, maxnode);
        countmemory(current_fbl, counterofmemory);
        for (int i = 0; i < current_fbl->total_worker_number; i++)
        {
            if (counterofmemory[maxnode] < counterofmemory[i])
                maxnode = i;
            if (counterofmemory[minnode] > counterofmemory[i])
                minnode = i;
        }
    }
}

file_position_type **collect_position_buffer(parallel_first_buffer_layer *current_fbl, int *counterofmemory)
{
    file_position_type **position_collect = malloc(sizeof(file_position_type *) * current_fbl->total_worker_number);
    int *countermemory = malloc(sizeof(int) * current_fbl->total_worker_number);

    for (int i = 0; i < current_fbl->total_worker_number; i++)
    {
        position_collect[i] = malloc(sizeof(file_position_type) * counterofmemory[i]);
        countermemory[i] = 0;
    }

    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        if (current_fbl->soft_buffers[i].initialized)
        {
            for (int j = 0; j < current_fbl->total_worker_number; j++)
            {
                if (current_fbl->soft_buffers[i].buffer_size[j] != 0)
                {
                    memcpy(&(position_collect[j][countermemory[j]]), current_fbl->soft_buffers[i].pos_records[j], sizeof(file_position_type) * current_fbl->soft_buffers[i].buffer_size[j]);
                    countermemory[j] += current_fbl->soft_buffers[i].buffer_size[j];
                }
            }
        }
    }
    file_position_type tmp;
    /*
    for (int i = 0; i < current_fbl->total_worker_number; i++)
    {
        for (int j = 0; j < countermemory[i]; j++)
        {
            for (int k = j+1; k < countermemory[i]; k++)
            {
                if (position_collect[i][j]>position_collect[i][k])
                {
                    tmp=position_collect[i][j];
                    position_collect[i][j]=position_collect[i][k];
                    position_collect[i][k]=tmp;
                }
            }
        }
    }
    */
    return position_collect;
}

node_list *index_creation_mpi_botao_uni_input_fast(const char *ifilename, long int ts_num, isax_index *index, int myrank, int round, float brate)
{
    // fprintf(stderr, ">>> Indexing: %s\n", ifilename);

    FILE *ifile;
    ts_type *rawfiletransfer;
    COUNT_INPUT_TIME_START
    ifile = fopen(ifilename, "rb");
    COUNT_INPUT_TIME_END
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);
    file_position_type total_records = sz / index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    node_list *returnnodelist = malloc(sizeof(node_list));
    index->sax_file = NULL;

    long int ts_loaded = 0;
    int i, node_counter = 0;
    pthread_t threadid[maxquerythread];
    unsigned long memorycounter;

    sax_type *saxcache = malloc(sizeof(sax_type) * index->settings->paa_segments * ts_num);
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);

    // set the thread on decided cpu

    parallel_first_buffer_layer *current_fbl = initialize_pRecBuf(index->settings->initial_fbl_buffer_size,
                                                                  pow(2, index->settings->paa_segments),
                                                                  index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    int number_counter = 1;
    int comm_sz, my_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    // printf("the size is %d\n",comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    current_fbl->total_worker_number = comm_sz;

    int *nodecollecter = malloc(sizeof(int) * pow(2, index->settings->paa_segments));
    int *nodedistributer = malloc(sizeof(int) * pow(2, index->settings->paa_segments));
    int tmp_collecter = 0;
    int numberofbuffer = pow(2, index->settings->paa_segments);
    for (int i = 0; i < numberofbuffer; i++)
    {
        nodecollecter[i] = 0;
        nodedistributer[i] = 0;
    }

    COUNT_OUTPUT_TIME_START
    ts_type *ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    for (unsigned long j = 0; j < ts_num; j++)
    {
        sax_type *sax = &(saxcache[j * index->settings->paa_segments]);
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        if (sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
                        index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                        index->settings->sax_bit_cardinality) == SUCCESS)
        {
            root_mask_type first_bit_mask = 0x00;
            CREATE_MASK(first_bit_mask, index, sax);
            nodecollecter[first_bit_mask] += 1;
        }
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    int *nodeamount = malloc(sizeof(int) * numberofbuffer);
    for (int i = 0; i < numberofbuffer; i++)
    {
        nodeamount[i] = i;
    }

    for (int i = 0; i < numberofbuffer; i++)
    {
        for (int j = i; j < numberofbuffer; j++)
        {
            if (nodecollecter[nodeamount[i]] < nodecollecter[nodeamount[j]])
            {
                int tmppp;
                tmppp = nodeamount[i];
                nodeamount[i] = nodeamount[j];
                nodeamount[j] = tmppp;
            }
        }
    }

    for (int i = 0; i < numberofbuffer; i++)
    {
        if (nodecollecter[DecimaltoGray(i)] != 0)
        {
            nodedistributer[DecimaltoGray(i)] = tmp_collecter;
            tmp_collecter = (tmp_collecter + 1) % comm_sz;
        }
    }

    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        parallel_fbl_soft_buffer *current_buffer = &(current_fbl->soft_buffers[i]);
        if (nodecollecter[i] != 0)
        {
            current_buffer->initialized = 1;
            current_buffer->pos_records = malloc(sizeof(file_position_type *) * comm_sz);
            current_buffer->buffer_size = malloc(sizeof(int) * comm_sz);
            current_buffer->max_buffer_size = malloc(sizeof(int) * comm_sz);
            current_buffer->max_buffer_size[0] = 0;
            for (int j = 0; j < comm_sz; j++)
            {
                current_buffer->pos_records[j] = NULL;
                current_buffer->buffer_size[j] = 0;
            }
            // int x=GraytoDecimal(i);
            current_buffer->pos_records[nodedistributer[i]] = malloc(sizeof(file_position_type) * nodecollecter[i]);
        }
    }

    file_position_type pos;
    fseek(ifile, 0L, SEEK_SET);
    for (unsigned long j = 0; j < ts_num; j++)
    {
        sax_type *sax = &(saxcache[j * index->settings->paa_segments]);
        // pos = ftell(ifile);
        // fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);

        root_mask_type first_bit_mask = 0x00;
        CREATE_MASK(first_bit_mask, index, sax);
        parallel_fbl_soft_buffer *current_buffer = &(current_fbl->soft_buffers[(int)first_bit_mask]);
        current_buffer->pos_records[nodedistributer[first_bit_mask]][current_buffer->buffer_size[nodedistributer[first_bit_mask]]] = j;
        current_buffer->buffer_size[nodedistributer[first_bit_mask]]++;
    }

    int *counterofmemory = malloc(sizeof(int) * comm_sz);
    int *counterofnode = malloc(sizeof(int) * comm_sz);
    for (int i = 0; i < comm_sz; i++)
    {
        counterofmemory[i] = 0;
        counterofnode[i] = 0;
    }
    fseek(ifile, 0L, SEEK_SET);
    int x;

    for (int i = 0; i < round; i++)
    {
        distribute_node_data(&(current_fbl->soft_buffers[nodeamount[i]]), current_fbl->total_worker_number);
    }
    countmemory(current_fbl, counterofmemory);
    data_distribute_input_2(current_fbl, counterofmemory, round, brate);
    // file_position_type **position_collect=NULL;
    // position_collect=collect_position_buffer(current_fbl,counterofmemory);

    returnnodelist->data_amount = counterofmemory[myrank];
    // printf("the memory size is %d\n",counterofmemory[myrank]);

    fseek(ifile, 0L, SEEK_SET);
    rawfiletransfer = malloc(sizeof(ts_type) * index->settings->timeseries_size * counterofmemory[myrank]);
    returnnodelist->rawfile = rawfiletransfer;
    memorycounter = 0;
    // fread(rawfiletransfer, sizeof(ts_type), index->settings->timeseries_size*counterofmemory[i%comm_sz], ifile);
    for (unsigned long j = 0; j < ts_num; j++)
    {
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        root_mask_type first_bit_mask = 0x00;
        sax_type *sax = &(saxcache[j * index->settings->paa_segments]);
        CREATE_MASK(first_bit_mask, index, sax);
        if (current_fbl->soft_buffers[first_bit_mask].max_buffer_size[0] == 0)
        {
            if (nodedistributer[first_bit_mask] == myrank)
            {
                memcpy(&rawfiletransfer[memorycounter * index->settings->timeseries_size], ts, sizeof(ts_type) * index->settings->timeseries_size);
                memorycounter++;
            }
        }
        else
        {

            if ((current_fbl->soft_buffers[first_bit_mask].max_buffer_size[0] - 1) % comm_sz == myrank)
            {
                memcpy(&rawfiletransfer[memorycounter * index->settings->timeseries_size], ts, sizeof(ts_type) * index->settings->timeseries_size);
                memorycounter++;
            }
            current_fbl->soft_buffers[first_bit_mask].max_buffer_size[0]++;
        }
    }
    fclose(ifile);
    free(saxcache);
    COUNT_OUTPUT_TIME_END
    return returnnodelist;
}

node_list *preprocessing_botao_to_manos(const char *ifilename, long int ts_num, isax_index *index, int myrank, int group_node_number, int round, float brate)
{
    // fprintf(stderr, ">>> Indexing: %s\n", ifilename);

    FILE *ifile;
    ts_type *rawfiletransfer;
    COUNT_INPUT_TIME_START
    ifile = fopen(ifilename, "rb");
    COUNT_INPUT_TIME_END
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);
    file_position_type total_records = sz / index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    node_list *returnnodelist = malloc(sizeof(node_list));
    index->sax_file = NULL;

    long int ts_loaded = 0;
    int i, node_counter = 0;
    pthread_t threadid[maxquerythread];
    unsigned long memorycounter;

    sax_type *saxcache = malloc(sizeof(sax_type) * index->settings->paa_segments * ts_num);
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);

    // set the thread on decided cpu

    parallel_first_buffer_layer *current_fbl = initialize_pRecBuf(index->settings->initial_fbl_buffer_size,
                                                                  pow(2, index->settings->paa_segments),
                                                                  index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    int number_counter = 1;
    int comm_sz;
    comm_sz = group_node_number;
    // printf("the size is %d\n",comm_sz);
    current_fbl->total_worker_number = comm_sz;

    int *nodecollecter = malloc(sizeof(int) * pow(2, index->settings->paa_segments));
    int *nodedistributer = malloc(sizeof(int) * pow(2, index->settings->paa_segments));
    int tmp_collecter = 0;
    int numberofbuffer = pow(2, index->settings->paa_segments);
    for (int i = 0; i < numberofbuffer; i++)
    {
        nodecollecter[i] = 0;
        nodedistributer[i] = 0;
    }

    COUNT_OUTPUT_TIME_START
    ts_type *ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    for (unsigned long j = 0; j < ts_num; j++)
    {
        sax_type *sax = &(saxcache[j * index->settings->paa_segments]);
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        if (sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
                        index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                        index->settings->sax_bit_cardinality) == SUCCESS)
        {
            root_mask_type first_bit_mask = 0x00;
            CREATE_MASK(first_bit_mask, index, sax);
            nodecollecter[first_bit_mask] += 1;
        }
    }

    int *nodeamount = malloc(sizeof(int) * numberofbuffer);
    for (int i = 0; i < numberofbuffer; i++)
    {
        nodeamount[i] = i;
    }

    for (int i = 0; i < numberofbuffer; i++)
    {
        for (int j = i; j < numberofbuffer; j++)
        {
            if (nodecollecter[nodeamount[i]] < nodecollecter[nodeamount[j]])
            {
                int tmppp;
                tmppp = nodeamount[i];
                nodeamount[i] = nodeamount[j];
                nodeamount[j] = tmppp;
            }
        }
    }

    for (int i = 0; i < numberofbuffer; i++)
    {
        if (nodecollecter[DecimaltoGray(i)] != 0)
        {
            nodedistributer[DecimaltoGray(i)] = tmp_collecter;
            tmp_collecter = (tmp_collecter + 1) % comm_sz;
        }
    }

    for (int i = 0; i < current_fbl->number_of_buffers; i++)
    {
        parallel_fbl_soft_buffer *current_buffer = &(current_fbl->soft_buffers[i]);
        if (nodecollecter[i] != 0)
        {
            current_buffer->initialized = 1;
            current_buffer->pos_records = malloc(sizeof(file_position_type *) * comm_sz);
            current_buffer->buffer_size = malloc(sizeof(int) * comm_sz);
            current_buffer->max_buffer_size = malloc(sizeof(int) * comm_sz);
            current_buffer->max_buffer_size[0] = 0;
            for (int j = 0; j < comm_sz; j++)
            {
                current_buffer->pos_records[j] = NULL;
                current_buffer->buffer_size[j] = 0;
            }
            // int x=GraytoDecimal(i);
            current_buffer->pos_records[nodedistributer[i]] = malloc(sizeof(file_position_type) * nodecollecter[i]);
        }
    }

    file_position_type pos;
    fseek(ifile, 0L, SEEK_SET);
    for (unsigned long j = 0; j < ts_num; j++)
    {
        sax_type *sax = &(saxcache[j * index->settings->paa_segments]);
        // pos = ftell(ifile);
        // fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);

        root_mask_type first_bit_mask = 0x00;
        CREATE_MASK(first_bit_mask, index, sax);
        parallel_fbl_soft_buffer *current_buffer = &(current_fbl->soft_buffers[(int)first_bit_mask]);
        current_buffer->pos_records[nodedistributer[first_bit_mask]][current_buffer->buffer_size[nodedistributer[first_bit_mask]]] = j;
        current_buffer->buffer_size[nodedistributer[first_bit_mask]]++;
    }

    int *counterofmemory = malloc(sizeof(int) * comm_sz);
    int *counterofnode = malloc(sizeof(int) * comm_sz);
    for (int i = 0; i < comm_sz; i++)
    {
        counterofmemory[i] = 0;
        counterofnode[i] = 0;
    }
    fseek(ifile, 0L, SEEK_SET);
    int x;

    for (int i = 0; i < round; i++)
    {
        distribute_node_data(&(current_fbl->soft_buffers[nodeamount[i]]), current_fbl->total_worker_number);
    }
    countmemory(current_fbl, counterofmemory);
    data_distribute_input_2(current_fbl, counterofmemory, round, brate);
    // file_position_type **position_collect=NULL;
    // position_collect=collect_position_buffer(current_fbl,counterofmemory);

    returnnodelist->data_amount = counterofmemory[myrank];
    // printf("the memory size is %d\n",counterofmemory[myrank]);

    fseek(ifile, 0L, SEEK_SET);
    rawfiletransfer = malloc(sizeof(ts_type) * index->settings->timeseries_size * counterofmemory[myrank]);
    returnnodelist->rawfile = rawfiletransfer;
    memorycounter = 0;
    // fread(rawfiletransfer, sizeof(ts_type), index->settings->timeseries_size*counterofmemory[i%comm_sz], ifile);
    for (unsigned long j = 0; j < ts_num; j++)
    {
        fread(ts, sizeof(ts_type), index->settings->timeseries_size, ifile);
        root_mask_type first_bit_mask = 0x00;
        sax_type *sax = &(saxcache[j * index->settings->paa_segments]);
        CREATE_MASK(first_bit_mask, index, sax);
        if (current_fbl->soft_buffers[first_bit_mask].max_buffer_size[0] == 0)
        {
            if (nodedistributer[first_bit_mask] == myrank)
            {
                memcpy(&rawfiletransfer[memorycounter * index->settings->timeseries_size], ts, sizeof(ts_type) * index->settings->timeseries_size);
                memorycounter++;
            }
        }
        else
        {

            if ((current_fbl->soft_buffers[first_bit_mask].max_buffer_size[0] - 1) % comm_sz == myrank)
            {
                memcpy(&rawfiletransfer[memorycounter * index->settings->timeseries_size], ts, sizeof(ts_type) * index->settings->timeseries_size);
                memorycounter++;
            }
            current_fbl->soft_buffers[first_bit_mask].max_buffer_size[0]++;
        }
    }
    fclose(ifile);
    free(saxcache);
    COUNT_OUTPUT_TIME_END
    return returnnodelist;
}

/*Botao to Manos: End copy*/

/**
 * @brief Density aware distribution of the data
 * 
 * @param ifilename 
 * @param ts_num 
 * @param index 
 * @param parallelism_in_subtree 
 * @param node_groups 
 * @param total_node_groups 
 * @param total_nodes_per_nodegroup 
 * @return float* 
 */
float *index_creation_node_groups_chatzakis_botao_prepro(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree, pdr_node_group *node_groups, int total_node_groups, int total_nodes_per_nodegroup)
{
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series // expected_records

    file_position_type total_records_per_group = total_records; // manolis
    // if each node has the whole dataset into its disk, must calculate and seek into the appropriate file position
    if (!mpi_already_splitted_dataset && total_node_groups > 1)
    {
        total_records_per_group = total_records / total_node_groups; // if we have odd number of node, the last node will get one time series extra
    }

    fseek(ifile, 0L, SEEK_SET);

    if (!IS_EVEN(total_node_groups) && (FIND_NODE_GROUP(my_rank, total_nodes_per_nodegroup) == total_node_groups - 1))
    { // odd number of nodes. So the last node on ranks will process one time series more
        total_records_per_group++;
    }

    if (total_records_per_group < ts_num)
    {
        fprintf(stderr, "[MC] - Index creation: File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "raw_file" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    float *rawfile; //!= malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    //!my_rank FIND_NODE_GROUP(my_rank, total_nodes_per_nodegroup)
    node_list *rawdatanodelist = preprocessing_botao_to_manos(ifilename, ts_num*total_node_groups, index, FIND_NODE_GROUP(my_rank, total_nodes_per_nodegroup), total_node_groups, 4000, 1.05f); //! check again!!!!
    rawfile = rawdatanodelist->rawfile; //!

    COUNT_INPUT_TIME_START

    long long int possition_to_file = 0;
    possition_to_file = ts_num * (sizeof(float) * time_series_size) * file_part_to_load; // 1024 it is need it !not to be used with multinode

    if (!mpi_already_splitted_dataset && comm_sz > 1) // for parallel queries, every node should create all index
    {
        int my_new_rank = my_rank;

        possition_to_file = FIND_NODE_GROUP(my_new_rank, total_nodes_per_nodegroup) * ts_num * (sizeof(float) * time_series_size);

        if (!IS_EVEN(comm_sz) && IS_LAST_NODE(my_new_rank, comm_sz))
        {                                                                                                                                    // odd number of nodes. So the last node on ranks will process one time series more
            possition_to_file = FIND_NODE_GROUP(my_new_rank, total_nodes_per_nodegroup) * (ts_num - 1) * (sizeof(float) * time_series_size); // else it will not take the one extra times series that is need it to process
        }
    }

    int returned_val = fseek(ifile, possition_to_file, SEEK_SET); // manolis
    if (returned_val != 0)
    {
        printf("Error on fseek()\n");
        exit(1);
    }

    size_t read_number; //= fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile); // int -> size_t

    read_number = rawdatanodelist->data_amount; //!!!!
    ts_num =  rawdatanodelist->data_amount; //!!!!!

    // manolis
    if ((read_number / time_series_size) != ts_num)
    { // time_series_size := 128 or 256
        //printf("Must read: %ld but it reads %ld times series\n", ts_num, read_number / time_series_size);
        //printf("Read times series != than the times series to be read\n");
        //exit(-1);
    }
    COUNT_INPUT_TIME_END
    // printf("[MC] - DEBUG - INPUT TIME END: After reading the dataset calculations: %f\n", total_input_time);

    if (my_rank == MASTER)
    {
        printf("[MC] - Master: Index Creation with %d threads.\n", maxreadthread);
    }

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas(index->settings->initial_fbl_buffer_size, pow(2, index->settings->paa_segments), index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxreadthread];                                                                         //[MC] - Changed it to use maxReadThread
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxreadthread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num

    unsigned long next_block_to_process = 0;
    int node_counter = 0; // required for tree construction using fai

    volatile unsigned long *next_iSAX_group = calloc(index->fbl->max_total_size, sizeof(unsigned long)); // EKOSMAS ADDED - 02 NOVEMBER 2020

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    // pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxreadthread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < maxreadthread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        input_data[i].parallelism_in_subtree = parallelism_in_subtree;
        input_data[i].next_iSAX_group = next_iSAX_group; // EKOSMAS ADDED - 02 NOVEMBER 2020

        input_data[i].rawfile = rawfile;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    // printf("[MC]: Max Query Thread: %d\n", maxquerythread);
    for (int i = 0; i < maxreadthread; i++)
    {
        pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new_ekosmas, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxreadthread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity_ekosmas(index, ts_num);

    return rawfile;
}
