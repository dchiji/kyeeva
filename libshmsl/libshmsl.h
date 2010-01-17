#ifndef __LIBSHMSL_H__
#define __LIBSHMSL_H__

typedef struct skiplist {
    unsigned int key;    // 3bytes: offset of datablock, 1byte: number of blocks
    unsigned int value;    // 3bytes: offset of datablock, 1byte: number of blocks

    unsigned int smaller[8];
    unsigned int bigger[8];

    unsigned int membership_vector;
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

#endif
