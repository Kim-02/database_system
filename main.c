#include <stdio.h>
#include "table_split.h"
int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <input.tbl>\n", argv[0]);
        return 1;
    }

    return split_by_pagesize(argv[1]);
}
