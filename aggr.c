// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <limits.h>
#include "groupby.h"

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

static long long ll_of_str(char const *str)
{
    return strtoll(str, NULL, 0);   // TODO: error check?
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
 * Rem
 */

static size_t rem_size(void)
{
    return 0;
}

static void rem_ctor(void *v_)
{
    (void)v_;
}

static void rem_fold(void *v_, char const *current)
{
    (void)v_;
    (void)current;
}

static char const *rem_finalize(void *v_)
{
    (void)v_;
    return "";
}

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

static void avg_fold(void *v_, char const *current)
{
    struct avg_value *v = v_;
    v->nb_values ++;
    v->sum += ll_of_str(current);
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
 * Min
 */

static void min_ctor(void *v_)
{
    long long *v = v_;
    *v = LLONG_MAX;
}

static void min_fold(void *v_, char const *current)
{
    long long *v = v_;
    long long const c = ll_of_str(current);
    if (c < *v) *v = c;
}

/*
 * Max
 */

static void max_ctor(void *v_)
{
    long long *v = v_;
    *v = LLONG_MIN;
}

static void max_fold(void *v_, char const *current)
{
    long long *v = v_;
    long long const c = ll_of_str(current);
    if (c > *v) *v = c;
}

/*
 * Sum
 */

static void sum_ctor(void *v_)
{
    long long *v = v_;
    *v = 0;
}

static void sum_fold(void *v_, char const *current)
{
    long long *v = v_;
    *v += ll_of_str(current);
}

/*
 * First
 */

static void first_fold(void *v_, char const *current)
{
    struct str_value *v = v_;
    if (! v->str) str_value_set(v, current);
}

/*
 * Last
 */

static void last_fold(void *v_, char const *current)
{
    struct str_value *v = v_;
    str_value_set(v, current);
}

/*
 * Smallest
 */

static void smallest_fold(void *v_, char const *current)
{
    struct str_value *v = v_;
    if (! v->str || strcmp(v->str, current) > 0) str_value_set(v, current);
}

/*
 * Greatest
 */

static void greatest_fold(void *v_, char const *current)
{
    struct str_value *v = v_;
    if (! v->str || strcmp(v->str, current) < 0) str_value_set(v, current);
}

/*
 * Table of all available aggr functions
 */

struct aggr_func aggr_funcs[] = {
    { { rem_size, rem_ctor, rem_fold, rem_finalize }, "rem" },
    { { avg_size, avg_ctor, avg_fold, avg_finalize }, "avg" },
    { { ll_size, min_ctor, min_fold, ll_finalize }, "min" },
    { { ll_size, max_ctor, max_fold, ll_finalize }, "max" },
    { { ll_size, sum_ctor, sum_fold, ll_finalize }, "sum" },
    { { str_size, str_ctor, first_fold, str_finalize }, "first" },
    { { str_size, str_ctor, last_fold, str_finalize }, "last" },
    { { str_size, str_ctor, smallest_fold, str_finalize }, "smallest" },
    { { str_size, str_ctor, greatest_fold, str_finalize }, "greatest" },
};

unsigned nb_aggr_funcs = SIZEOF_ARRAY(aggr_funcs);

