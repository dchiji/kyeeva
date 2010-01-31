#ifndef __LIBSHMSL_H__
#define __LIBSHMSL_H__

#define BLOCK_SIZE 24
#define NEIGHBOR_N 12
#define LOCAL_NEIGHBOR_N 4

unsigned int G_MEMBERSHIP_VECTOR;

typedef struct skiplist {
    unsigned int nobusy_flag: 1;
    unsigned int canuse_neighbor_smaller: 3;
    unsigned int canuse_neighbor_bigger: 3;
    unsigned int fill :1;

    unsigned int membership_vector;

    unsigned int key;    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int value;    // 3bytes: offset of datablock, 1byte: number of blocks

    unsigned int global_smaller;    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int global_bigger;    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int local_smaller[LOCAL_NEIGHBOR_N];    // offset of skiplist
    unsigned int local_bigger[LOCAL_NEIGHBOR_N];    // offset of skiplist
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
int shmsl_free(shmsl_t *info);
unsigned char *shmsl_value(skiplist_t *p);
unsigned char *shmsl_get(shmsl_t *info, unsigned char *key);
int strcmp_with_diff(unsigned char *str1, unsigned char *str2, unsigned int str1_size, unsigned int str2_size);
skiplist_t *select_best(skiplist_t *sl_blocks, unsigned char *db_blocks, unsigned int *neighbor, unsigned char *key, unsigned int neighbor_n, unsigned int key_size, char S_or_B);

#endif
