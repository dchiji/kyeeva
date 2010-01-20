#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>

#include "libshmsl.h"
#include "stack.h"

shmsl_t *shmsl_init(char *id_pathname, unsigned int skiplist_size, unsigned int datablock_size)
{
    key_t skiplist_key;
    key_t datablock_key;
    shmsl_t *info;
    
    if((info = (shmsl_t *)malloc(sizeof(shmsl_t))) == NULL) {
        printf("[libshmsl/shmsl_init/malloc]: Failed memory allocation\n");
        return NULL;
    }

    {
        FILE *fp;

        if((fp = fopen(pathname, "r")) == NULL) {
            printf("[libshmsl/shmsl_init/fopen]: identification file <%s> is not found.\n", id_pathname);
            return NULL;
        } else {
            fclose(fp);
        }
    }

    if((skiplist_key = ftok(pathname, *(int *)"SL  ")) == -1) {
        perror("[libshmsl/shmsl_init/ftok]");
        return NULL;
    }
    info->skiplist_id = shmget(skiplist_key,
                               sizeof(block_header_t) + sizeof(skiplist_t) * skiplist_size,
                               IPC_CREAT);
    if(info->skiplist_id == -1) {
        perror("[libshmsl/shmsl_init/shmget]");
        return NULL;
    }

    if((info->skiplist = shmat(info->skiplist_id, 0, 0)) == -1) {
        perror("[libshmsl/shmsl_init/shmat]");
        return NULL;
    }
    if((info->skiplist->unused_stack_id = stack_init(pathname, skiplist_size, NULL)) == -1) {
        return NULL;
    }
    info->skiplist->size    = skiplist_size;
    info->skiplist->tail    = 0;
    info->skiplist->self_id = info->skiplist_id;
    info->skiplist->next_id = 0;

    if((datablock_key = ftok(pathname, *(int *)"DB  ")) == -1) {
        perror("[libshmsl/shmsl_init/ftok]");
        return NULL;
    }
    info->datablock_id = shmget(datablock_key,
                                sizeof(block_header_t) + sizeof(datablock_t) * datablock_size,
                                IPC_CREAT);
    if(info->datablock_id == -1) {
        perror("[libshmsl/shmsl_init/shmget]");
        return NULL;
    }

    if((info->datablock = shmat(info->datablock_id, 0, 0)) == -1) {
        perror("[libshmsl/shmsl_init/shmat]");
        return NULL;
    }
    if((info->datablock->unused_stack_id = stack_init(pathname, datablock_size, NULL)) == -1) {
        return NULL;
    }
    info->datablock->size    = datablock_size;
    info->datablock->tail    = 0;
    info->datablock->self_id = info->datablock_id;
    info->datablock->next_id = 0;

    return info;
}

int shmsl_delete(shmsl_t *info)
{
    shmdt(info->skiplist);
    shmdt(info->datablock);
    shmctl(info->skiplist_id, IPC_RMID, 0);
    shmctl(info->datablock_id, IPC_RMID, 0);

    free(info);

    return 1;
}

unsigned char *shmsl_get(shmsl_t *info, const unsigned char *key)
{
    unsigned char *node;

    if(info == NULL) {
        printf("[libshmsl/shmsl_get]: Invalid argument\n");
        return NULL;
    }

    if(info->skiplist == NULL) {
        if((sl_header = shmat(info->skiplist_id, 0, 0)) == -1) {
            perror("[libshmsl/shmsl_init/shmat]");
            return NULL;
        }
        info->skiplist = shmat(info->skiplist_id, 0, 0);
    }

    node = info->skiplist->blocks;

    while(node != NULL) {
        unsigned int offset    = node->key >> (sizeof(int) / 4);
        unsigned int block_n   = (node->key << (sizeof(int) * 3 / 4)) >> (sizeof(int) * 3 / 4);
        unsigned char *block   = datablock_get(offset, block_n);

        unsigned int key_size  = strlen(key);
        unsigned int data_size = *(int *)block;
        unsigned char *data    = block + sizeof(int);

        int result             = strcmp_with_diff(data, key, data_size, key_size);

        if(result == -1) {
            unsigned char *p = malloc(data_size);

            if(p == NULL) {
                printf("[libshmsl/shmsl_init/malloc]: Failed memory allocation\n");
                return NULL;
            }

            memcpy(p, data, sizeof(char) * BLOCK_SIZE * block_n);
            return p;
        }

        if(result == key_size || key[result] < data[result]){
            node = select_best(node->local_smaller, key, LOCAL_NEIGHBOR_N, key_size);
        } else if(key[result] > data[result]) {
            node = select_best(node->local_bigger, key, LOCAL_NEIGHBOR_N, key_size);
        }
    }
}

int strcmp_with_diff(char *str1, char *str2, unsigned int str1_size, unsigned int str2_size)
{
    int max = (str1_size < str2_size)?str1_size:str2_size;
    int i;
    int j;

    for(i = 0; i < max; i += 4) {
        if(max - i < 4) {
            for(j = 0; j + i < max; j++) {
                if(str1[i + j] != str[i + j]) {
                    return i + j;
                }
            }
        } else {
            if(*((int *)str1 + i / 4) != *((int *)str + i / 4)) {
                for(j = 0; j < 4; j++) {
                    if(str1[i + j] != str[i + j]) {
                        return i + j;
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

skiplist_t *select_best(unsigned char *data_block, unsigned int *neighbor, unsigned char *key, unsigned int neighbor_n, unsigned int key_size)
{
    int i;

    for(i = 0; i < neighbor_n; i++) {
        unsigned int offset = neighbor[i] >> (sizeof(int) / 4);
        unsigned int block_n = (neighbor[i] << (sizeof(int) * 3 / 4)) >> (sizeof(int) * 3 / 4);

    }
}
