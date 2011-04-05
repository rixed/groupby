// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include "groupby.h"

/*
 * Avg
 */

struct avg_value {
    unsigned nb_values;
    long long sum;
}

static void *avg_new(struct aggr_func *f)
{
    struct avg_value *v = malloc(sizeof(*v));
    if (! v) return NULL;

    v->nb_values = 0;
    v->sum = 0;
}

static void avg_fold(void *v_, struct field_value const *current)
{
    struct avg_value *v = v_;
    v->nb_values ++;
    v->sum += current->num;
}

static void avg_finalize(void *v_)
{
    struct avg_value *v = v_;
    return (union field_value){ .num = v->num / v->nb_values };
}

struct aggr_func aggr_funcs[] = {
    { { avg_new, avg_fold, avg_finalize }, true, "avg" },
#   if 0
    { { min_new, min_fold, min_finalize }, true, "min" },
    { { max_new, max_fold, max_finalize }, true, "max" },
    { { sum_new, sum_fold, sum_finalize }, true, "sum" },
    { { first_new, first_fold, first_finalize }, false, "first" },
    { { last_new, last_fold, last_finalize }, false, "last" },
    { { smallest_new, smallest_fold, smallest_finalize }, false, "smallest" },
    { { greatest_new, greatest_fold, greatest_finalize }, false, "greatest" },
#   endif
};

unsigned nb_aggr_funcs = SIZEOF_ARRAY(aggr_funcs);

