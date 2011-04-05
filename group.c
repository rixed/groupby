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

static struct group *group_new(struct groups *groups, char const **values, unsigned const nb_values, struct row_conf const *conf, unsigned h)
{
//    if (debug) fprintf(stderr, "Building new group for %u grouped values\n", nb_values);
    struct group *group = malloc(sizeof(*group));
    if (! group) {
        fprintf(stderr, "Cannot malloc %zu bytes for a group\n", sizeof(*group));
        goto err1;
    }

    group->conf = conf;

    group->nb_grouped_fields = nb_values;
    group->grouped_values = malloc(nb_values * sizeof(*group->grouped_values));
    if (! group->grouped_values) {
        fprintf(stderr, "Cannot malloc %u values for a group\n", nb_values);
        goto err2;
    }

    for (unsigned gv = 0; gv < nb_values; gv ++) {
        group->grouped_values[gv] = strdup(values[gv]);
//        if (debug) fprintf(stderr, "  grouped value %u = '%s'\n", gv, group->grouped_values[gv]);
        if (! group->grouped_values[gv]) {
            fprintf(stderr, "Cannot strdup for a group\n");
            // err2 -> err3' which frees all duped strs
            goto err2;
        }
    }

    group->nb_aggr_fields = 0;  // will be incremented when we actually see the fields
    group->aggr_values = malloc(group->conf->nb_aggr_fields * sizeof(*group->aggr_values));
    if (! group->aggr_values) {
        fprintf(stderr, "Cannot malloc %u values for a group\n", group->conf->nb_aggr_fields);
        goto err3;
    }

    unsigned aggr_field_no = 0;
    for (unsigned f = 0; f < group->conf->nb_fields; f++) {
        if (! group->conf->fields[f].aggr) continue;
        assert(aggr_field_no < group->conf->nb_aggr_fields);
        group->aggr_values[aggr_field_no] = group->conf->fields[f].aggr->ops.new();
        if (! group->aggr_values[aggr_field_no]) {
            goto err3;
        }
        aggr_field_no ++;
    }

    SLIST_INSERT_HEAD(groups->hash + h, group, entry);
    groups->length ++;
    if (debug && 0 == (groups->length & 0xfff)) {
        fprintf(stderr, "%u groups\n", groups->length);
    }

    return group;

err3:
    free(group->grouped_values);
err2:
    free(group);
err1:
    return NULL;
}

static bool same_grouped_values(unsigned nb_values, char const **v1, char const **v2)
{
    if (nb_values == 0) return true;
    if (0 != strcmp(*v1, *v2)) return false;
    return same_grouped_values(nb_values-1, v1+1, v2+1);
}

static uint32_t hash_single(char const *value, uint32_t prev)
{
    return hashlittle(value, strlen(value), prev);
}

static unsigned hash_values(char const **values, unsigned nb_values, unsigned hash_size)
{
    unsigned h = 0x12345678;
    while (nb_values --) {
        h = hash_single(values[nb_values], h);
    }

    return h % hash_size;
}

struct group *group_find_or_create(struct groups *groups, char const **values, unsigned const nb_values, struct row_conf const *conf)
{
    assert(nb_values > 0);
    unsigned h = hash_values(values, nb_values, SIZEOF_ARRAY(groups->hash));

    struct group *group;
    SLIST_FOREACH(group, groups->hash + h, entry) {
        if (nb_values != group->nb_grouped_fields) continue;
        if (same_grouped_values(nb_values, group->grouped_values, values)) break;
    }

    if (! group) {
        group = group_new(groups, values, nb_values, conf, h);
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

