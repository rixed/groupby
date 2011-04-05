// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <assert.h>
#include "groupby.h"
#include "config.h"

static int aggr_of_str(char const *str, struct aggr_func const **aggr)
{
    for (unsigned f = 0; f < nb_aggr_funcs; f++) {
        if (strcasecmp(str, aggr_funcs[f].name) == 0) {
            *aggr = aggr_funcs+f;
            return 0;
        }
    }
    fprintf(stderr, "Unknown function '%s'", str);
    return -1;
}

static void set_range(struct row_conf *row_conf, unsigned first, unsigned last, struct aggr_func const *aggr, bool inv)
{
    assert(first > 0 && last > 0);
    if (last < first) {
        unsigned tmp = first;
        first = last; last = tmp;
    }

    for (unsigned f = first-1; f < last; f++) {
        assert(f < row_conf->nb_field_confs);
        row_conf->confs[f].need_num = aggr->need_num;
        row_conf->confs[f].aggr = aggr;
    }
}

static int set_fieldspec_conf(struct row_conf *row_conf, char const *start, char const *stop, struct aggr_func const *aggr, bool inv)
{
    char const *const spec = start;
    if (start >= stop) return 0;
    if (*start == '!') set_fieldspec_conf(row_conf, start+1, stop, aggr, inv);

    unsigned first = 0, last = 0;   // invalid field numbers
    char *eoi;
    // read first
    first = strtoul(start, &eoi, 0);
    if (eoi == start) {
        first = 1;
    } else {
        start = eoi;
    }

    if (*start == '-') {
        start ++;
        last = strtoul(start, &eoi, 0);
        start = eoi;
    } else {
        last = first;
    }

    if (start < stop && *start != ',')
bad_spec:
        fprintf(stderr, "Bad field spec: %s", spec);
        return -1;
    }

    set_range(frow_conf, first, last, aggr, inv);

    if (start >= stop) return 0;

    return set_fieldspec_conf(row_conf, start+1, stop, aggr, inv);
}

static int parse_conf(struct row_conf *row_conf, char const *opt)
{
    // First look for the function to set
    struct aggr_func const *aggr = NULL;
    char const *colon = strchr(opt, ':');
    if (colon) {
        if (0 != aggr_of_str(colon+1, &aggr)) {
            return -1;
        }
    } else {
        colon = rawmemchr(opt, '\0');
    }

    // Now set this aggr function for each specified field
    return set_fieldspec_conf(row_conf, opt, colon, aggr, false);
}

int main(int nb_args, char **args)
{
    struct row_conf *row_conf = row_conf_new(1000); // as a first version

    for (unsigned a = 1; a < nb_args; a++) {
        if (strcasecmp(args+a, "-h") == 0 || strcasecmp(args+a, "--help") == 0) {
            syntax();
            return EXIT_SUCCESS;
        } else if (strcasecmp(args+a, "-a") == 0) {
            if (0 != parse_conf(row_conf, args+a)) {
                fprintf(stderr, "Try --help");
                return EXIT_FAILURE;
            }
        } else {
            fprintf(stderr, "Unknown option '%s'\n", args+a);
            syntax();
            return EXIT_FAILURE;
        }
    }

    if (0 != do_groupby(row_conf)) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
