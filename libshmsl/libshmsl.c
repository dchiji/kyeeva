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
