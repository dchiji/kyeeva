#ifndef __LIBSHMSL_H__
#define __LIBSHMSL_H__

#define BLOCK_SIZE 24

typedef struct skiplist {
    unsigned int membership_vector;

    unsigned int key;    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int value;    // 3bytes: offset of datablock, 1byte: number of blocks

    unsigned int global_smaller;    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int global_bigger;    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int local_smaller[8];    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int local_bigger[8];    // 3bytes: offset of datablock, 1byte: number of blocks
} skiplist_t;

typedef struct block_header {
    unsigned int size;
    unsigned int tail;

    int self_id;
    int unused_stack_id;
    int next_id;

    unsigned char blocks[];
} block_header_t;

typedef struct shmsl {
    int skiplist_id;
    int datablock_id;
    unsigned int skiplist_size;
    unsigned int datablock_size;

    block_header_t *skiplist;
    block_header_t *datablock;
} shmsl_t;

shmsl_t *shmsl_init(char *id_pathname, unsigned int skiplist_size, unsigned int datablock_size);
int shmsl_delete(shmsl_t *info);
unsigned char *shmsl_get(shmsl_t *info, const unsigned char *key);
int strcmp_with_diff(char *str1, char *str2, unsigned int str1_size, unsigned int str2_size);
skiplist_t *select_best(unsigned char *data_block, unsigned int *neighbor, unsigned char *key, unsigned int neighbor_n, unsigned int key_size);

#endif
