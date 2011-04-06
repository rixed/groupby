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
    struct row_conf *conf;
    size_t const size = sizeof(*conf) + nb_fields_max * sizeof(conf->fields[0]);
    conf = malloc(size);
    if (! conf) {
        fprintf(stderr, "Cannot malloc %zu bytes for row conf\n", size);
        return NULL;
    }

    conf->nb_fields = nb_fields_max;
    conf->nb_aggr_fields = 0;
    conf->aggr_tot_size = 0;
    for (unsigned f = 0; f < conf->nb_fields; f++) {
        conf->fields[f] = NULL;
    }

    return conf;
}

void row_conf_finalize(unsigned nb_max_fields, struct row_conf *conf)
{
    conf->nb_fields = nb_max_fields;
    for (unsigned f = 0; f < conf->nb_fields; f++) {
        if (! conf->fields[f]) continue;
        conf->nb_aggr_fields ++;
        conf->aggr_cumul_size[f] = conf->aggr_tot_size;
        conf->aggr_tot_size += conf->fields[f]->ops.size();
    }
}

static struct state {
    struct row_conf const *conf;
    unsigned field_no, record_no;
    int input, output;
    struct groups groups;
    char delimiter;
    char const *values[];    // as many values as conf->nb_fields
} *state;

static struct state *state_new(struct row_conf const *conf, int input, int output, char delimiter)
{
    struct state *state;
    size_t size = sizeof(*state) + conf->nb_fields * sizeof(state->values[0]);
    state = malloc(size);
    if (! state) {
        fprintf(stderr, "Cannot alloc %zu bytes for parse state\n", size);
        return NULL;
    }

    state->conf = conf;
    state->input = input;
    state->output = output;
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

    if (state->field_no >= state->conf->nb_fields) {
        fprintf(stderr, "More than %u records\n", state->conf->nb_fields);
        exit(EXIT_FAILURE);
    }

    state->values[state->field_no] = field;

    state->field_no ++;
}

static void record_cb(void *state_)
{
    struct state *state = state_;

    static char key_buf[MAX_RECORD_LENGTH];
    struct key_str key = { .str = key_buf, .len = 0 };

    for (unsigned f = 0; f < state->field_no; f++) {
        if (state->conf->fields[f]) continue;
        key_str_append(&key, state->values[f]);
    }

    // Look for this group in our hash (will create a new one if not found)
    struct group *group = group_find_or_create(&state->groups, &key, state->conf);

    if (group) {
        // update the aggregate values in the group
        assert(state->field_no <= state->conf->nb_fields);
        for (unsigned f = 0; f < state->field_no; f++) {
            if (!state->conf->fields[f]) continue;
            // aggregate this value
            state->conf->fields[f]->ops.fold(group->values + state->conf->aggr_cumul_size[f], state->values[f]);
        }
        if (state->field_no > group->nb_fields) group->nb_fields = state->field_no;
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
    FILE *output = fdopen(state->output, "w");

    // extract grouped values from key_str
    char const *grouped_values[NB_MAX_FIELDS];
    unsigned const nb_grouped_values = key_str_extract(&group->grouped_values, grouped_values);
    unsigned g = 0;
    for (unsigned f = 0; f < group->nb_fields; f++) {
        void const *src;
        if (state->conf->fields[f]) {
            src = state->conf->fields[f]->ops.finalize(group->values + state->conf->aggr_cumul_size[f]);
        } else {
            assert(g < nb_grouped_values);
            src = grouped_values[g++];
        }
        char const *const quote = must_quote(src, state->delimiter) ? "\"":"";
        fprintf(output, "%s%s%s%s", f > 0 ? delimiter:"", quote, (char *)src, quote);
    }
    fprintf(output, "\n");
}

static ssize_t reader(void *dst, size_t dst_size, void *state_)
{
    struct state *state = state_;
    ssize_t const r = read(state->input, dst, dst_size);
    if (r < 0) perror("read");
    return r;
}

int do_groupby(struct row_conf const *row_conf, char delimiter, int input, int output)
{
    state = state_new(row_conf, input, output, delimiter);
    if (! state) return -1;

    struct csv csv;
    if (0 != csv_ctor(&csv, nb_max_fields*NB_MAX_FIELD_LENGTH, delimiter, reader, state)) {
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
