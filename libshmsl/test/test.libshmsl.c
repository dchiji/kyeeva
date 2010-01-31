#include <stdio.h>
#include <string.h>
#include "../libshmsl.h"

int test_strcmp_with_diff()
{
    int i;
    int result;
    const unsigned char *str[][2] = {{"five", "four"},
                                     {"seven", "sevem"},
                                     {"long", "short"},
                                     {"hoge", "bar"},
                                     {"same", "same"}};

    for(i = 0; i < 5; i++) {
        result = strcmp_with_diff(str[i][0], str[i][1], strlen(str[i][0]), strlen(str[i][1]));
        printf("strcmp_with_diff \"%s\", \"%s\" = %d\n", str[i][0], str[i][1], result);
    }

    return 1;
}

int test_shmsl()
{
    shmsl_t *p = shmsl_init("/home/daiki/.test", 32, 32);
    printf("p->skiplist_id = %d\n", p->skiplist_id);
    printf("p->datablock_id = %d\n", p->datablock_id);
    printf("p->skiplist_size = %d\n", p->skiplist_size);
    printf("p->datablock_size = %d\n", p->datablock_size);
    printf("\n");

    return 1;
}

int main()
{
    test_strcmp_with_diff();
    printf("\n");
    test_shmsl();
}
