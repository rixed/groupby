// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "groupby.h"
#include "jhash.h"

int groups_ctor(struct groups *groups)
{
    for (unsigned h = 0; h < SIZEOF_ARRAY(groups->hash); h++) {
        SLIST_INIT(groups->hash + h);
    }
    groups->length = 0;
    return 0;
}

void groups_dtor(struct groups *groups)
{
    (void)groups;
    // free all groups ?
}

void key_str_append(struct key_str *key, char const *v)
{
    unsigned l = strlen(v);
    memcpy(key->str+key->len, v, l+1);
    key->len += l+1;
    assert(key->len < MAX_RECORD_LENGTH);
}

unsigned key_str_extract(struct key_str const *key, char const *res[NB_MAX_FIELDS])
{
    unsigned r = 0;
    unsigned len = key->len;
    char const *str = key->str;
    while (len > 0) {
        res[r++] = str;
        char const *next = rawmemchr(str, '\0')+1;
        len -= next - str;
        str = next;
    }
    return r;
}

bool key_str_eq(struct key_str const *a, struct key_str const *b)
{
    return a->len == b->len && 0 == memcmp(a->str, b->str, a->len);
}

static struct group *group_new(struct groups *groups, struct key_str *key, struct row_conf const *conf, unsigned h)
{
    if (debug) fprintf(stderr, "Building new group for key of len %u\n", key->len);

    struct group *group;
    size_t const size = sizeof(*group) + conf->aggr_tot_size;
    group = malloc(size);
    if (! group) {
        fprintf(stderr, "Cannot malloc %zu bytes for a group\n", size);
        goto err1;
    }

    group->grouped_values.len = key->len;
    group->grouped_values.str = malloc(key->len);
    if (! group->grouped_values.str) {
        fprintf(stderr, "Cannot malloc for key of length %u\n", key->len);
        goto err2;
    }
    memcpy(group->grouped_values.str, key->str, key->len);

    group->nb_fields = 0;  // will be incremented when we actually see the fields
    for (unsigned f = 0; f < conf->nb_fields; f++) {
        if (! conf->fields[f]) continue;
        conf->fields[f]->ops.ctor(group->values + conf->aggr_cumul_size[f]);
    }

    SLIST_INSERT_HEAD(groups->hash + h, group, entry);
    groups->length ++;
    if (debug && 0 == (groups->length & 0xfff)) {
        fprintf(stderr, "%u groups\n", groups->length);
    }

    return group;

err2:
    free(group);
err1:
    return NULL;
}

static unsigned hash_values(char const *value, unsigned len, unsigned hash_size)
{
    return hashlittle(value, len, 0x12345678) % hash_size;
}

struct group *group_find_or_create(struct groups *groups, struct key_str *key, struct row_conf const *conf)
{
    unsigned h = hash_values(key->str, key->len, SIZEOF_ARRAY(groups->hash));

    struct group *group;
    SLIST_FOREACH(group, groups->hash + h, entry) {
        if (key_str_eq(&group->grouped_values, key)) break;
    }

    if (! group) {
        group = group_new(groups, key, conf, h);
    }

    return group;
}

void groups_foreach(struct groups *groups, void (*cb)(struct group *, void *), void *data)
{
    for (unsigned h = 0; h < SIZEOF_ARRAY(groups->hash); h++) {
        struct group *group;
        SLIST_FOREACH(group, groups->hash + h, entry) {
            cb(group, data);
        }
    }
}

