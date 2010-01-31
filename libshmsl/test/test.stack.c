#include <stdio.h>
#include "../stack.h"

int main()
{
    stack_header_t *h;
    int id = stack_init("/home/daiki/.test", 30, &h);
    int item0;
    int item1;

    if(id == -1) {
        return -1;
    }
    stack_push(h, 100);
    stack_push(h, 200);
    stack_pop(h, &item0);
    stack_pop(h, &item1);
    printf("%d, %d\n", item0, item1);

    stack_delete(h, 0);

    return 0;
}
