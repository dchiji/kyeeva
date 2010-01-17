#include <sys/types>
#include <sys/ipc.h>

#include "stack.h"

int stack_init(const char *pathname, unsigned int size)
{
    key_t key;
    int stack_id;
    stack_header_t *tmp;

    {
        FILE *fp;

        if((fp = fopen(pathname, "r")) == NULL) {
            printf("libshmsl error: identification file <%s> is not found.\n", id_pathname);
            return NULL;
        } else {
            fclose(fp);
        }
    }
    key = ftok(pathname, *(int *)"STCK");
    stack_id = shmget(key, sizeof(stack_header_t) + sizeof(int) * size, IPC_CREAT);

    tmp       = shmat(stack_id, 0, 0);
    tmp->size = size;
    tmp->top  = 0;

    shmdt(tmp);

    return stack_id;
}

int stack_push(stack_header_t *header, unsigned int item)
{
    unsigned int size = header->size;
    unsigned int top = header->top;
    unsigned char *stack = header->stack;

    while(!__sync_bool_compare_and_swap(&header->top, top, top + 1)) {
        if((top = header->top) >= size) {
            printf("libshmsl error: stack overflow\n");
            return -1;
        }
    }
    stack[top] = item;

    return item;
}

unsigned int stack_pop(stack_header_t *header)
{
    unsigned int top = header->top;
    unsigned int item = header->stack[top];

    if(!__sync_bool_compare_and_swap(&header->top, top, top - 1)) {
        while(!__sync_bool_compare_and_swap(stack + top, item, item)) {
            while(!__sync_bool_compare_and_swap(&header->top, top, top - 1)) {
                if((top = header->top) >= size) {
                    printf("libshmsl error: stack overflow\n");
                    return -1;
                }
                item = header->stack[top];
            }
        }
    }

    return item;
}

