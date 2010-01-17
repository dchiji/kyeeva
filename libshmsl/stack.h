#ifndef __STACK_H__
#define __STACK_H__

typedef struct stack_header {
    unsigned int size;
    unsigned int top;
    unsigned char stack[];
} stack_header_t;

#endif
