// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <limits.h>
#include "groupby.h"

/*
 * Avg
 */

struct avg_value {
    unsigned nb_values;
    long long sum;
};

static size_t avg_size(void)
{
    return sizeof(struct avg_value);
}

static void avg_ctor(void *v_)
{
    struct avg_value *v = v_;
    v->nb_values = 0;
    v->sum = 0;
}

static void avg_fold(void *v_, union field_value const *current)
{
    struct avg_value *v = v_;
    v->nb_values ++;
    v->sum += current->num;
}

static char const *avg_finalize(void *v_)
{
    struct avg_value *v = v_;
    assert(v->nb_values > 0);

    static char str[32];
    snprintf(str, sizeof(str), "%lld", (v->sum + v->nb_values/2) / v->nb_values);
    return str;
}

/*
 * Helper for common values
 */

static size_t ll_size(void)
{
    return sizeof(long long);
}

static char const *ll_finalize(void *v_)
{
    long long *v = v_;
    static char str[32];
    snprintf(str, sizeof(str), "%lld", *v);
    return str;
}

struct str_value {
    char *str;
    size_t size;
};

static size_t str_size(void)
{
    return sizeof(struct str_value);
}

static void str_ctor(void *v_)
{
    struct str_value *v = v_;
    v->str = NULL;
    v->size = 0;
}

static char const *str_finalize(void *v_)
{
    struct str_value *v = v_;
    return v->str;
}

static void str_value_set(struct str_value *v, char const *str)
{
    unsigned size = strlen(str) + 1;
    if (size > v->size) {
        v->size = 4 * size + 1;
        char *new = malloc(v->size);
        if (new) {
            strcpy(new, str);
            if (v->str) free(v->str);
            v->str = new;
        } else {
            // ?
        }
    } else {
        strcpy(v->str, str);
    }
}

/*
 * Min
 */

static void min_ctor(void *v_)
{
    long long *v = v_;
    *v = LLONG_MAX;
}

static void min_fold(void *v_, union field_value const *current)
{
    long long *v = v_;
    if (current->num < *v) *v = current->num;
}

/*
 * Max
 */

static void max_ctor(void *v_)
{
    long long *v = v_;
    *v = LLONG_MIN;
}

static void max_fold(void *v_, union field_value const *current)
{
    long long *v = v_;
    if (current->num > *v) *v = current->num;
}

/*
 * Sum
 */

static void sum_ctor(void *v_)
{
    long long *v = v_;
    *v = 0;
}

static void sum_fold(void *v_, union field_value const *current)
{
    long long *v = v_;
    *v += current->num;
}

/*
 * First
 */

static void first_fold(void *v_, union field_value const *current)
{
    struct str_value *v = v_;
    if (! v->str) str_value_set(v, current->str);
}

/*
 * Last
 */

static void last_fold(void *v_, union field_value const *current)
{
    struct str_value *v = v_;
    str_value_set(v, current->str);
}

/*
 * Smallest
 */

static void smallest_fold(void *v_, union field_value const *current)
{
    struct str_value *v = v_;
    if (! v->str || strcmp(v->str, current->str) > 0) str_value_set(v, current->str);
}

/*
 * Greatest
 */

static void greatest_fold(void *v_, union field_value const *current)
{
    struct str_value *v = v_;
    if (! v->str || strcmp(v->str, current->str) < 0) str_value_set(v, current->str);
}

/*
 * Table of all available aggr functions
 */

struct aggr_func aggr_funcs[] = {
    { { avg_size, avg_ctor, avg_fold, avg_finalize }, true, "avg" },
    { { ll_size, min_ctor, min_fold, ll_finalize }, true, "min" },
    { { ll_size, max_ctor, max_fold, ll_finalize }, true, "max" },
    { { ll_size, sum_ctor, sum_fold, ll_finalize }, true, "sum" },
    { { str_size, str_ctor, first_fold, str_finalize }, false, "first" },
    { { str_size, str_ctor, last_fold, str_finalize }, false, "last" },
    { { str_size, str_ctor, smallest_fold, str_finalize }, false, "smallest" },
    { { str_size, str_ctor, greatest_fold, str_finalize }, false, "greatest" },
};

unsigned nb_aggr_funcs = SIZEOF_ARRAY(aggr_funcs);

