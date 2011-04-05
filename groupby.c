// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include "groupby.h"

struct row_conf *row_conf_new(unsigned nb_fields_max)
{
    size_t const size = sizeof(*row_conf) + nb_fields_max * sizeof(row_conf->confs);
    struct row_conf *row_conf = malloc(size);
    if (! row_conf) {
        fprintf(stderr, "Cannot malloc %zu bytes\n", size);
        return NULL;
    }

    row_conf->nb_fields = nb_fields_max;
    for (unsigned f = 0; f < row_conf->nb_fields; f++) {
        row_conf->fields[f].is_num = false;
        row_conf->fields[f].aggr = NULL;
    }

    return row_conf;
}

int do_groupby(struct row_conf const *row_conf)
{
    (void)row_conf;
    return -1;
}
