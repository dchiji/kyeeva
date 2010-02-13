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
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/stat.h>
#include "stack.h"

int stack_init(const char *pathname, unsigned int size, stack_header_t **p)
{
    key_t key;
    int stack_id;
    stack_header_t *tmp;

    {
        FILE *fp;

        if((fp = fopen(pathname, "r")) == NULL) {
            printf("[shmsl/stack_init/fopen]: Identification file <%s> is not found\n", pathname);
            return -1;
        } else {
            fclose(fp);
        }
    }
    if((key = ftok(pathname, *(int *)"STCK")) == -1) {
        perror("[shmsl/stack_init/ftok]");
        return -1;
    }

    if((stack_id = shmget(key, sizeof(stack_header_t) + sizeof(int) * size, IPC_CREAT|S_IRUSR)) == -1){
        perror("[shmsl/stack_init/shmget]");
        return -1;
    }
    if((int)(tmp = (stack_header_t *)shmat(stack_id, NULL, 0)) == -1) {
        perror("[shmsl/stack_init/shmat]");
        return -1;
    }
    tmp->size    = size;
    tmp->self_id = stack_id;
    tmp->top     = 0;

    if(p != NULL) {
        *p = tmp;
    } else {
        shmdt(tmp);
    }

    return stack_id;
}

int stack_push(stack_header_t *header, unsigned int item)
{
    unsigned int size = header->size;
    unsigned int top = header->top;
    unsigned int *stack = header->stack;

    while(!__sync_bool_compare_and_swap(&header->top, top, top + 1)) {
        if((top = header->top) >= size) {
            printf("[shmsl/stack_push]: Stack overflow\n");
            return -1;
        }
    }
    stack[top] = item;

    return item;
}

unsigned int stack_pop(stack_header_t *header, unsigned int *p)
{
    unsigned int size = header->size;
    unsigned int top = header->top;
    unsigned int *stack = header->stack;
    unsigned int item = header->stack[top - 1];

    if(top == 0) {
        printf("[shmsl/stack_pop]: Stack is empty\n");
        return -1;
    }

    if(!__sync_bool_compare_and_swap(&header->top, top, top - 1)) {
        top = header->top;
        item = header->stack[top - 1];

        do {
            while(!__sync_bool_compare_and_swap(&header->top, top, top - 1)) {
                if((top = header->top) >= size) {
                    printf("[shmsl/stack_pop]: Stack overflow\n");
                    return -1;
                }
                if(top == 0) {
                    printf("[shmsl/stack_pop]: Stack is empty\n");
                    return -1;
                }
                item = header->stack[top - 1];
            }
        } while(!__sync_bool_compare_and_swap(stack + top - 1, item, item));
    }

    if(p != NULL) {
        *p = item;
    }
    return 1;
}

int stack_delete(stack_header_t *header, int id)
{
    if(header != NULL) {
        if(id == 0) {
            id = header->self_id;
        }
        shmdt(header);
    }
    shmctl(id, 0, 0);

    return 1;
}

