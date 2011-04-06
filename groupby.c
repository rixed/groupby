// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include "groupby.h"

struct row_conf *row_conf_new(unsigned nb_fields_max)
{
    struct row_conf *row_conf;
    size_t const size = sizeof(*row_conf) + nb_fields_max * sizeof(row_conf->fields[0]);
    row_conf = malloc(size);
    if (! row_conf) {
        fprintf(stderr, "Cannot malloc %zu bytes for row conf\n", size);
        return NULL;
    }

    row_conf->nb_fields = nb_fields_max;
    row_conf->nb_aggr_fields = 0;
    for (unsigned f = 0; f < row_conf->nb_fields; f++) {
        row_conf->fields[f].need_num = false;
        row_conf->fields[f].aggr = NULL;
    }

    return row_conf;
}

static struct state {
    struct row_conf const *conf;
    unsigned field_no, record_no;
    int file;
    struct groups groups;
    char delimiter;
    char const *values[];    // as many values as conf->nb_fields
} *state;

static struct state *state_new(struct row_conf const *conf, int file, char delimiter)
{
    struct state *state;
    size_t size = sizeof(*state) + conf->nb_fields * sizeof(state->values[0]);
    state = malloc(size);
    if (! state) {
        fprintf(stderr, "Cannot alloc %zu bytes for parse state\n", size);
        return NULL;
    }

    state->conf = conf;
    state->file = file;
    state->delimiter = delimiter;
    state->field_no = state->record_no = 0;
    if (0 != groups_ctor(&state->groups)) {
        free(state);
        return NULL;
    }

    return state;
}

static void state_del(struct state *state)
{
    groups_ctor(&state->groups);
    free(state);
}

static void field_cb(void *field, size_t field_len, void *state_)
{
    if (debug) fprintf(stderr, "got field '%s'\n", (char *)field);
    (void)field_len;
    struct state *state = state_;

    assert(state->field_no < state->conf->nb_fields);

    state->values[state->field_no] = field;

    state->field_no ++;
}

static void record_cb(void *state_)
{
    struct state *state = state_;

    static char const *grouped_values[NB_MAX_FIELDS];
    unsigned nb_groupby_values = 0;

    for (unsigned f = 0; f < state->field_no; f++) {
        if (state->conf->fields[f].aggr) continue;
        assert(nb_groupby_values < SIZEOF_ARRAY(grouped_values));
        grouped_values[nb_groupby_values++] = state->values[f];
    }

    struct group *group = NULL;
    if (0 == nb_groupby_values) {
        fprintf(stderr, "Discard record %u which countains no grouped value\n", state->record_no+1);
    } else {
        // Look for this group in our hash (will create a new one if not found)
        group = group_find_or_create(&state->groups, grouped_values, nb_groupby_values, state->conf);
    }

    if (group) {
        // update the aggregate values in the group
        unsigned nb_aggr_values = 0;
        for (unsigned f = 0; f < state->field_no; f++) {
            if (!state->conf->fields[f].aggr) continue;
            // aggregate this value
            union field_value current;
            if (state->conf->fields[f].need_num) {
                char *end;
                current.num = strtoll(state->values[f], &end, 0);
                if (*end != '\0') {
                    fprintf(stderr, "Cannot parse '%s' as numeric value (field %u, line %u)\n", state->values[f], f+1, state->record_no+1);
                    return;
                }
            } else {
                current.str = state->values[f];
            }
            assert(nb_aggr_values <= group->conf->nb_aggr_fields);
            state->conf->fields[f].aggr->ops.fold(group->aggr_values[nb_aggr_values], &current);
            nb_aggr_values ++;
        }
        if (nb_aggr_values > group->nb_aggr_fields) group->nb_aggr_fields = nb_aggr_values;
    }

    state->field_no = 0;
    state->record_no ++;
}

static bool must_quote(char const *str, char const delimiter)
{
    for (; *str; str++) {
        if (*str == '"' || *str == '\n' || *str == delimiter) return true;
    }
    return false;
}

static void dump_group(struct group *group, void *state_)
{
    struct state *state = state_;
    char delimiter[2] = { state->delimiter, '\0' };

    unsigned grouped_field_no = 0, aggr_field_no = 0;

    for (unsigned f = 0; f < group->nb_aggr_fields + group->nb_grouped_fields; f++) {
        void const *src;
        if (group->conf->fields[f].aggr) {
            src = group->conf->fields[f].aggr->ops.finalize(group->aggr_values[aggr_field_no]);
            aggr_field_no ++;
        } else {
            src = group->grouped_values[grouped_field_no];
            grouped_field_no ++;
        }
        char const *const quote = must_quote(src, state->delimiter) ? "\"":"";
        fprintf(stdout, "%s%s%s%s", f > 0 ? delimiter:"", quote, (char *)src, quote);
    }
    fprintf(stdout, "\n");
}

static ssize_t reader(void *dst, size_t dst_size, void *state_)
{
    struct state *state = state_;
    ssize_t const r = read(state->file, dst, dst_size);
    if (r < 0) perror("read");
    return r;
}

int do_groupby(struct row_conf const *row_conf, char delimiter, int file)
{
    state = state_new(row_conf, file, delimiter);
    if (! state) return -1;

    struct csv csv;
    if (0 != csv_ctor(&csv, NB_MAX_FIELDS*NB_MAX_FIELD_LENGTH, delimiter, reader, state)) {
        assert(0);
        return -1;
    }
    int err = csv_parse(&csv, field_cb, record_cb);
    csv_dtor(&csv);

    if (err) return -1;

    groups_foreach(&state->groups, dump_group, state);

    state_del(state);

    return 0;
}
