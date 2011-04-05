// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#ifndef GROUPBY_H_110404
#define GROUPBY_H_110404
#include <stdbool.h>

#define SIZEOF_ARRAY(x) (sizeof(x)/sizeof(*(x)))

union field_value {
    long long num;
    struct str_value {
        char const *start, *stop;
    } str;
};

extern struct aggr_func {
    struct aggr_ops {
        // returns a new object to be given to fold
        void *(* new)(struct aggr_func *);
        // update the object previously returned by new with a new value
        void (*fold)(void *old, struct field_value const *current);
        // get the field value of the object
        union field_value finalize(void *v);
    } const *ops;
    bool need_num;
    char const *name;
} aggr_funcs[];

extern unsigned nb_aggr_funcs;

struct field_conf {
    bool need_num;
    aggr_func *aggr;    // If NULL then keep the field
};

struct row_conf {
    unsigned nb_fields;
    struct field_conf fields[];  // beware!
};

// Return an empty row_conf
struct row_conf *row_conf_new(unsigned nb_fields_max);

int do_groupby(struct row_conf const *);

#endif
