// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#ifndef GROUPBY_H_110404
#define GROUPBY_H_110404
#include <stdbool.h>
#include <sys/queue.h>

#define SIZEOF_ARRAY(x) (sizeof(x)/sizeof(*(x)))
#define NB_MAX_FIELDS 60
#define NB_MAX_FIELD_LENGTH 50000

extern bool debug;

union field_value {
    long long num;
    char const *str;
};

extern struct aggr_func {
    struct aggr_ops {
        // returns a new object to be given to fold
        void *(* new)(void);
        // update the object previously returned by new with a new value
        void (*fold)(void *old, union field_value const *current);
        // get the final value of the object (as a string)
        char const *(*finalize)(void *v);
    } const ops;
    bool need_num;
    char const *name;
} aggr_funcs[];

extern unsigned nb_aggr_funcs;

struct row_conf {
    unsigned nb_fields;
    unsigned nb_aggr_fields;    // how many of which have an aggr function
    struct field_conf {
        bool need_num;
        struct aggr_func const *aggr; // If NULL then group by this field
    } fields[];  // beware!
};

// Return an empty row_conf
struct row_conf *row_conf_new(unsigned nb_fields_max);

int do_groupby(struct row_conf const *, char delimiter, int file);

struct group {
    SLIST_ENTRY(group) entry;
    unsigned nb_grouped_fields;
    char const **grouped_values;
    struct row_conf const *conf;
    unsigned nb_aggr_fields;    // how many aggr_fields were observed
    void **aggr_values;  // number given by conf->nb_aggr_fields;
};

struct groups {
#   define GROUP_HASH_SIZE (0x10000)
    SLIST_HEAD(group_lists, group) hash[GROUP_HASH_SIZE];
    unsigned length;
};

int groups_ctor(struct groups *);
void groups_dtor(struct groups *);
struct group *group_find_or_create(struct groups *, char const **values, unsigned const nb_values, struct row_conf const *);
void groups_foreach(struct groups *, void (*cb)(struct group *, void *), void *);

struct csv {
    size_t buf_size;
    size_t max_row_size;
    unsigned datalen;
    unsigned upto;
    unsigned cursor;
    ssize_t (*reader)(void *, size_t, void *);
    bool eof;
    char *buffer;
    void *user_data;
    char delimiter;
};

int csv_ctor(struct csv *csv, size_t max_row_size, char delimiter, ssize_t (*reader)(void *, size_t, void *), void *);
void csv_dtor(struct csv *);
int csv_parse(struct csv *, void (*field_cb)(void *, size_t, void *), void (*record_cb)(void *));

#endif
