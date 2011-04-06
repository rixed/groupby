// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#ifndef GROUPBY_H_110404
#define GROUPBY_H_110404
#include <stdbool.h>
#include <sys/queue.h>

#define SIZEOF_ARRAY(x) (sizeof(x)/sizeof(*(x)))
#define NB_MAX_FIELDS 60
#define NB_MAX_FIELD_LENGTH 50000
#define MAX_RECORD_LENGTH (NB_MAX_FIELDS*(NB_MAX_FIELD_LENGTH+3))

extern bool debug;

union field_value {
    long long num;
    char const *str;
};

extern struct aggr_func {
    struct aggr_ops {
        // return the size of the internal object to be allocated
        size_t (* size)(void);
        // construct a new object to be given to fold
        void (* ctor)(void *);   // given pointer points to a space of AGGR_OBJ_SIZE bytes
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
    size_t aggr_cumul_size[NB_MAX_FIELDS];  // size of all values before this field
    size_t aggr_tot_size;
    struct field_conf {
        bool need_num;
        struct aggr_func const *aggr; // If NULL then group by this field
    } fields[];  // beware!
};

// Return an empty row_conf
struct row_conf *row_conf_new(unsigned nb_fields_max);

void row_conf_finalize(struct row_conf *);

int do_groupby(struct row_conf const *, char delimiter, int ifile, int ofile);

struct key_str {
    char *str;
    unsigned len;
};

void key_str_append(struct key_str *, char const *);
bool key_str_eq(struct key_str const *, struct key_str const *);
unsigned key_str_extract(struct key_str const *, char const *res[NB_MAX_FIELDS]);

struct group {
    SLIST_ENTRY(group) entry;
    struct key_str grouped_values;
    unsigned nb_fields;    // how many fields were observed, at max
    char values[];  // size given by conf->aggr_tot_size
};

struct groups {
#   define GROUP_HASH_SIZE (0x10000)
    SLIST_HEAD(group_lists, group) hash[GROUP_HASH_SIZE];
    unsigned length;
};

int groups_ctor(struct groups *);
void groups_dtor(struct groups *);
struct group *group_find_or_create(struct groups *, struct key_str *, struct row_conf const *);
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
