// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "groupby.h"
#include "config.h"

bool debug = false;

static int aggr_of_str(char const *str, struct aggr_func const **aggr)
{
    for (unsigned f = 0; f < nb_aggr_funcs; f++) {
        if (strcasecmp(str, aggr_funcs[f].name) == 0) {
            *aggr = aggr_funcs+f;
            return 0;
        }
    }
    fprintf(stderr, "Unknown function '%s'\n", str);
    return -1;
}

static void set_range(struct row_conf *row_conf, unsigned first, unsigned last, struct aggr_func const *aggr, bool inv)
{
    if (last < first) {
        unsigned tmp = first;
        first = last; last = tmp;
    }

    for (unsigned f = 0; f < row_conf->nb_fields; f++) {
        bool const in_between = f >= first && f <= last;
        if ((!inv && in_between) || (inv && !in_between)) {
            assert(f < row_conf->nb_fields);
            row_conf->fields[f].need_num = aggr ? aggr->need_num : false;
            row_conf->fields[f].aggr = aggr;
            if (aggr) {
                if (debug) fprintf(stderr, "field %u uses aggr function %s\n", f, aggr->name);
            } else {
                if (debug) fprintf(stderr, "field %u is groupped\n", f);
            }
        }
    }
}

static int set_fieldspec_conf(struct row_conf *row_conf, char const *start, char const *stop, struct aggr_func const *aggr, bool inv)
{
    char const *const spec = start;
    int const spec_len = stop - start;
    if (start >= stop) return 0;
    if (*start == '!') return set_fieldspec_conf(row_conf, start+1, stop, aggr, inv);

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
        if (eoi == start) {
            last = NB_MAX_FIELDS+1;
        } else {
            start = eoi;
        }
    } else {
        last = first;
    }

    if (start < stop && *start != ',') {
        fprintf(stderr, "Bad field spec: '%.*s'\n", spec_len, spec);
        return -1;
    }

    if (first == 0 || last == 0) {
        fprintf(stderr, "Fields are numbered from 1\n");
        return -1;
    }

    set_range(row_conf, first-1, last-1, aggr, inv);

    if (start >= stop) return 0;

    return set_fieldspec_conf(row_conf, start+1, stop, aggr, inv);
}

static int aggr_conf(struct row_conf *row_conf, char const *opt)
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

static int group_conf(struct row_conf *row_conf, char const *opt)
{
    return set_fieldspec_conf(row_conf, opt, opt + strlen(opt), NULL, false);
}

static void syntax(void)
{
    printf("groupby [-h | -a field_spec:function ... | -g field_spec] [-d char] [-i input] [-o output] [-v]\n"
           "\n"
           "where :\n"
           "  field_spec : n | n-m | -n | n- | field_spec,field_spec | !field_spec\n"
           "  n/m : field numbers (first field is 1)\n");
}

int main(int nb_args, char **args)
{
    struct row_conf *row_conf = row_conf_new(NB_MAX_FIELDS); // as a first version
    char delimiter = ',';
    int input = 0;
    int output = 1;

    for (int a = 1; a < nb_args; a++) {
        if (strcasecmp(args[a], "-h") == 0 || strcasecmp(args[a], "--help") == 0) {
            syntax();
            return EXIT_SUCCESS;
        } else if (strcasecmp(args[a], "-v") == 0 || strcasecmp(args[a], "--verbose") == 0) {
            debug = true;
        } else if (strcasecmp(args[a], "-a") == 0 && a < nb_args-1) {
            if (0 != aggr_conf(row_conf, args[a+1])) {
                fprintf(stderr, "Try --help");
                return EXIT_FAILURE;
            }
            a ++;
        } else if (strcasecmp(args[a], "-g") == 0 && a < nb_args-1) {
            if (0 != group_conf(row_conf, args[a+1])) {
                fprintf(stderr, "Try --help");
                return EXIT_FAILURE;
            }
            a ++;
        } else if (strcasecmp(args[a], "-d") == 0 && a < nb_args-1) {
            if (strlen(args[a+1]) != 1) {
                fprintf(stderr, "Delimiter must be a single char\n");
                return EXIT_FAILURE;
            }
            delimiter = args[a+1][0];
            if (debug) fprintf(stderr, "Delimiter is now '%c'\n", delimiter);
            a ++;
        } else if ((strcasecmp(args[a], "-i") == 0 || strcasecmp(args[a], "--input") == 0) && a < nb_args-1) {
            input = open(args[a+1], O_RDONLY);
            if (input < 0) {
                perror("open");
                return EXIT_FAILURE;
            }
            a ++;
        } else if ((strcasecmp(args[a], "-o") == 0 || strcasecmp(args[a], "--output") == 0) && a < nb_args-1) {
            output = open(args[a+1], O_WRONLY);
            if (output < 0) {
                perror("open");
                return EXIT_FAILURE;
            }
            a ++;
        } else {
            fprintf(stderr, "Unknown option '%s'\n", args[a]);
            syntax();
            return EXIT_FAILURE;
        }
    }

    row_conf_finalize(row_conf);

    if (0 != do_groupby(row_conf, delimiter, input, output)) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
