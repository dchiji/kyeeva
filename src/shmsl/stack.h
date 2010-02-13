#ifndef __STACK_H__
#define __STACK_H__

typedef struct stack_header {
    unsigned int size;
    int self_id;
    unsigned int top;
    unsigned int stack[];
} stack_header_t;

int stack_init(const char *pathname, unsigned int size, stack_header_t **p);
int stack_push(stack_header_t *header, unsigned int item);
unsigned int stack_pop(stack_header_t *header, unsigned int *p);
int stack_delete(stack_header_t *header, int id);

#endif
