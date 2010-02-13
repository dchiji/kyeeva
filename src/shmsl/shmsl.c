/*
 *  Copyright 2009~2010  CHIJIWA Daiki <daiki41@gmail.com>
 *  
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *  
 *       1. Redistributions of source code must retain the above copyright
 *          notice, this list of conditions and the following disclaimer.
 *  
 *       2. Redistributions in binary form must reproduce the above copyright
 *          notice, this list of conditions and the following disclaimer in
 *          the documentation and/or other materials provided with the
 *          distribution.
 *  
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE FREEBSD
 *  PROJECT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 *  TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
 *  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 *  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *
 * TODO:
 *     shmsl_putの実装
 *     shmsl_deleteの実装
 *     shmsl_delete, shmsl_put, shmsl_getをbusy_flagに対応させる
 *     deleted_flag
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>

#include "shmsl.h"
#include "stack.h"

shmsl_t *shmsl_init(char *pathname, unsigned int skiplist_size, unsigned int datablock_size)
{
    key_t skiplist_key;
    key_t datablock_key;
    shmsl_t *info;
    
    if((info = (shmsl_t *)malloc(sizeof(shmsl_t))) == NULL) {
        printf("[shmsl/shmsl_init/malloc]: Failed memory allocation\n");
        return NULL;
    }

    {
        FILE *fp;

        if((fp = fopen(pathname, "r")) == NULL) {
            printf("[shmsl/shmsl_init/fopen]: identification file <%s> is not found.\n", pathname);
            return NULL;
        } else {
            fclose(fp);
        }
    }

    if((skiplist_key = ftok(pathname, *(int *)"SL  ")) == -1) {
        perror("[shmsl/shmsl_init/ftok]");
        return NULL;
    }
    info->skiplist_id = shmget(skiplist_key,
                               sizeof(block_header_t) + sizeof(skiplist_t) * skiplist_size,
                               IPC_CREAT);
    if(info->skiplist_id == -1) {
        perror("[shmsl/shmsl_init/shmget]");
        return NULL;
    }

    if((signed int)(info->skiplist = (block_header_t *)shmat(info->skiplist_id, 0, 0)) == -1) {
        perror("[shmsl/shmsl_init/shmat]");
        return NULL;
    }
    if((info->skiplist->unused_stack_id = stack_init(pathname, skiplist_size, NULL)) == -1) {
        return NULL;
    }
    info->skiplist_size    = skiplist_size;
    info->skiplist->size    = skiplist_size;
    info->skiplist->tail    = 0;
    info->skiplist->self_id = info->skiplist_id;
    info->skiplist->next_id = 0;

    if((datablock_key = ftok(pathname, *(int *)"DB  ")) == -1) {
        perror("[shmsl/shmsl_init/ftok]");
        return NULL;
    }
    info->datablock_id = shmget(datablock_key,
                                sizeof(block_header_t) + BLOCK_SIZE * datablock_size,
                                IPC_CREAT);
    if(info->datablock_id == -1) {
        perror("[shmsl/shmsl_init/shmget]");
        return NULL;
    }

    if((int)(info->datablock = (block_header_t *)shmat(info->datablock_id, 0, 0)) == -1) {
        perror("[shmsl/shmsl_init/shmat]");
        return NULL;
    }
    if((info->datablock->unused_stack_id = stack_init(pathname, datablock_size, NULL)) == -1) {
        return NULL;
    }
    info->datablock_size    = datablock_size;
    info->datablock->size    = datablock_size;
    info->datablock->tail    = 0;
    info->datablock->self_id = info->datablock_id;
    info->datablock->next_id = 0;

    return info;
}

int shmsl_free(shmsl_t *info)
{
    shmdt(info->skiplist);
    shmdt(info->datablock);
    shmctl(info->skiplist_id, IPC_RMID, 0);
    shmctl(info->datablock_id, IPC_RMID, 0);

    free(info);

    return 1;
}

int shmsl_put(shmsl_t *info, unsigned char *key, unsigned char *value, unsigned int key_size, unsigned int value_size)
{
    unsigned int tail;
    unsigned char *key_db_ptr;
    unsigned char *value_db_ptr;
    skiplist_t *new_node;

    for(tail = info->tail; !__sync_bool_compare_and_swap(&info->tail, tail, tail + sizeof(skiplist_t)); tail = info->tail);
    new_node = info_block + tail;

    info->membership_vector = G_MEMBERSHIP_VECTOR;
    if((key_db_ptr = datablock_put(info->datablock->blocks + new_node->key, key, key_size)) == NULL) {
        return 0;
    }
    if((value_db_ptr = datablock_put(info->datablock->blocks + new_node->key, value, value_size)) == NULL) {
        return 0;
    }

    if(tail == 0) {
        new_node->canuse_neighbor_smaller = LOCAL_NEIGHBOR_N;
        new_node->canuse_neighbor_bigger = LOCAL_NEIGHBOR_N;
        new_node->nobusy_flag = 1;
    } else {
        unsigned int level; 
        skiplist_t *node;
        unsigned char *node_key;
        unsigned int node_key_size;
        int cmp_result;

        skiplist_t *smaller;
        skiplist_t *bigger;

        node = shmsl_get(info, key, key_size);
        node_key = shmsl_key(node, &node_key_size);

        cmp_result = strcmp_with_diff(node_key, key, node_key_size, key_size);

        if(result == -1) {
            datablock_put(datablock + node->value, value, value_size);
        } else if (node_key[cmp_result] < key[cmp_result]) {
            bigger = new_node->local_smaller[0]->local_bigger[0];

            for(level = 0; level < LOCAL_NEIGHBOR_N; level++) {
                node = seek_next_neighbor(node, membership_vector, level, 'S');

                new_node->local_smaller[level] = node;
                new_node->local_bigger[level] = node->local_bigger[level];

                node->local_bigger[level] = new_node;
            }
        } else {
            smaller = new_node->local_bigger[0]->local_smaller[0];

            for(level = 0; level < LOCAL_NEIGHBOR_N; level++) {
                node = seek_next_neighbor(node, membership_vector, level, 'B');

                new_node->local_bigger[level] = node;
                new_node->local_smaller[level] = node->local_smaller[level];

                node->local_smaller[level] = new_node;
            }
        }
    }
    return new_node;
}

unsigned char *shmsl_key(block_header_t *datablock, skiplist_t *node, int new_alloc_flag, int *key_size_ptr)
{
    unsigned char *data;
    unsigned int key_size;

    data = datablock->blocks + node->key;
    key_size = *data;
    key = data + sizeof(int);

    if(key_size_ptr != NULL) {
        *key_size_ptr = key_size;
    }

    if(new_alloc_flag) {
        unsigned char *p = (unsigned char *)malloc(sizeof(char) * key_size);

        if(p == NULL) {
            printf("[shmsl/shmsl_key/malloc]: Failed memory allocation\n");
            return NULL;
        }

        return memcpy(p, key, key_size);
    }

    return key;
}

unsigned char *shmsl_value(block_header_t *datablock, skiplist_t *node, int new_alloc_flag, int *value_size_ptr)
{
    unsigned int offset;
    unsigned char *data;
    unsigned char *value;
    unsigned int value_size;

    offset     = node->value >> (sizeof(int) / 4);
    data       = datablock->blocks + offset;
    value_size = *data;
    value      = data + sizeof(int);

    if(value_size_ptr != NULL) {
        *value_size_ptr = value_size;
    }

    if(new_alloc_flag) {
        unsigned char *p = (unsigned char *)malloc(data_size);

        if(p == NULL) {
            printf("[shmsl/shmsl_value/malloc]: Failed memory allocation\n");
            return NULL;
        }

        return memcpy(p, node->key, sizeof(char) * BLOCK_SIZE * block_n);
    } else {
        return value;
    }
}

skiplist_t *shmsl_get(shmsl_t *info, unsigned char *key, unsigned int key_size)
{
    skiplist_t *node;

    if(info == NULL) {
        printf("[shmsl/shmsl_get]: Invalid argument\n");
        return NULL;
    }

    if(info->tail == 0) {
        printf("[shmsl/shmsl_get]: Skip List is empty");
        return NULL;
    }

    if(info->skiplist == NULL) {
        if((int)(info->skiplist = (block_header_t *)shmat(info->skiplist_id, 0, 0)) == -1) {
            perror("[shmsl/shmsl_init/shmat]");
            return NULL;
        }
    }

    if(info->datablock == NULL) {
        if((int)(info->datablock = (block_header_t *)shmat(info->datablock_id, 0, 0)) == -1) {
            perror("[shmsl/shmsl_init/shmat]");
            return NULL;
        }
    }

    node = (skiplist_t *)info->skiplist->blocks;

    while(node != NULL) {
        unsigned int offset;
        unsigned int block_n;
        unsigned char *block;

        unsigned int data_size;
        unsigned char *data;

        int result;

        offset    = node->key >> (sizeof(int) / 4);
        block_n   = (node->key << (sizeof(int) * 3 / 4)) >> (sizeof(int) * 3 / 4);
        block     = info->datablock->blocks + offset;

        data_size = *(int *)block;
        data      = block + sizeof(int);

        result    = strcmp_with_diff(data, key, data_size, key_size);

        if(result == key_size || key[result] < data[result]){
            node = select_best((skiplist_t *)info->skiplist->blocks,
                               info->datablock->blocks,
                               node->local_smaller,
                               key,
                               LOCAL_NEIGHBOR_N,
                               key_size,
                               'S');
        } else if(key[result] > data[result]) {
            node = select_best((skiplist_t *)info->skiplist->blocks,
                               info->datablock->blocks,
                               node->local_bigger,
                               key,
                               LOCAL_NEIGHBOR_N,
                               key_size,
                               'B');
        }
    }

    return node;
}

int strcmp_with_diff(unsigned char *str1, unsigned char *str2, unsigned int str1_size, unsigned int str2_size)
{
    int max = (str1_size < str2_size)?str1_size:str2_size;
    int i;
    int j;

    for(i = 0; i < max; i++) {
        if(max - i < sizeof(int)) {
            for(j = 0; i * sizeof(int) + j < max; j++) {
                if(str1[i * sizeof(int) + j] != str2[i + j]) {
                    return i * sizeof(int) + j;
                }
            }
        } else {
            if(*((int *)str1 + i) != *((int *)str2 + i)) {
                for(j = 0; j < sizeof(int); j++) {
                    if(str1[i * sizeof(int) + j] != str2[i * sizeof(int) + j]) {
                        return i * sizeof(int) + j;
                    }
                }
            }
        }
    }

    if(str1_size == str2_size) {
        return -1;
    } else {
        return max - 1;
    }
}

skiplist_t *select_best(skiplist_t *sl_blocks, unsigned char *db_blocks, unsigned int *neighbor, unsigned char *key, unsigned int neighbor_n, unsigned int key_size, char S_or_B)
{
    skiplist_t *prev_node = NULL;
    int prev_result;

    int i;

    for(i = 0; i < neighbor_n; i++) {
        unsigned int sl_offset;
        skiplist_t *sl_node;

        unsigned int db_offset;
        unsigned int db_block_n;

        unsigned int neighbor_key_size;
        int sl_node_offset;
        unsigned char *neighbor_key;

        int result;

        sl_offset = neighbor[i];
        sl_node = sl_blocks + sl_offset;

        db_offset = sl_node->key >> 8;
        db_block_n = (sl_node->key << (sizeof(int) * 8 - 8)) >> (sizeof(int) * 8 - 8);

        neighbor_key_size = *(int *)(db_blocks + db_offset * BLOCK_SIZE);
        neighbor_key = db_blocks + db_offset * BLOCK_SIZE + sizeof(int);

        result = strcmp_with_diff(neighbor_key, key, neighbor_key_size, key_size);

        if(result == -1) {
            return sl_blocks + sl_node_offset;
        } else if(S_or_B == 'B' &&
                prev_result > result &&
                key[result] < neighbor_key[result]) {
            return prev_node;
        } else if(S_or_B == 'S' &&
                prev_result < result &&
                key[result] > neighbor_key[result]) {
            return prev_node;
        }

        prev_node = sl_node;
    }

    return prev_node;
}

unsigned char *datablock_put(unsigned char *p, unsigned char *new_data, unsigned int new_data_size)
{
}

