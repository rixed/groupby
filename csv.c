// -*- c-basic-offset: 4; c-backslash-column: 79; indent-tabs-mode: nil -*-
// vim:sw=4 ts=4 sts=4 expandtab
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "groupby.h"

int csv_ctor(struct csv *csv, size_t max_row_size, char delimiter, ssize_t (*reader)(void *, size_t, void *), void *user_data)
{
    csv->delimiter = delimiter;
    csv->max_row_size = max_row_size;
    csv->buf_size = 2*max_row_size;
    if (debug) fprintf(stderr, "new csv with max_row_len = %zu and buf_size = %zu\n", max_row_size, csv->buf_size);
    csv->buffer = malloc(csv->buf_size+1);
    csv->datalen = 0;
    csv->upto = 0;
    csv->cursor = 0;
    csv->eof = false;
    csv->user_data = user_data;
    csv->reader = reader;
    if (! csv->buffer) {
        fprintf(stderr, "Cannot malloc for row buffer\n");
        return -1;
    }
    csv->buffer[csv->buf_size] = '\0';  // so that we can use strchr and friends

    return 0;
}

void csv_dtor(struct csv *csv)
{
    free(csv->buffer);
}

static void csv_discard(struct csv *csv)
{
    if (csv->upto < csv->datalen) {
        if (debug) fprintf(stderr, "discarding from %u to %u\n", csv->upto, csv->datalen);
        memmove(csv->buffer, csv->buffer + csv->upto, csv->datalen - csv->upto);
        csv->datalen -= csv->upto;
        csv->cursor -= csv->upto;
    } else {
        csv->datalen = 0;
        csv->cursor = 0;
    }
    csv->upto = 0;
}

static void csv_feed(struct csv *csv)
{
    if (csv->eof) return;

    csv_discard(csv);

    unsigned const rem_size = csv->buf_size - csv->datalen;
    if (rem_size <= 0) return;
    if (debug) fprintf(stderr, "feeding csv while cursor=%u, datalen=%u, upto=%u\n", csv->cursor, csv->datalen, csv->upto);
    ssize_t r = csv->reader(csv->buffer + csv->datalen, rem_size, csv->user_data);
    if (r < 0) return;
    if (r == 0) {
        if (debug) fprintf(stderr, "hit end of file\n");
        csv->eof = true;
    } else if (r > 0) {
        if (debug) fprintf(stderr, "read %zd new bytes\n", r);
        csv->datalen += r;
    }

    if (debug) fprintf(stderr, "now cursor=%u, datalen=%u, upto=%u\n", csv->cursor, csv->datalen, csv->upto);
}

static int csv_find(struct csv *csv, char const *chars)
{
    for (; csv->cursor < csv->datalen; csv->cursor ++) {
        for (char const *c = chars; *c != '\0'; c++) {
            if (csv->buffer[csv->cursor] == *c) {
                return 0;
            }
        }
    }
    return -1;
}

int csv_parse(struct csv *csv, void (*field_cb)(void *, size_t, void *), void (*record_cb)(void *))
{
    char any_delimiter[3] = { csv->delimiter, '\n', 0 };
    unsigned lineno = 1;
    unsigned fieldno = 0;

    csv_feed(csv);
    while (! csv->eof || csv->upto < csv->datalen) {
        bool quoted = false;
            
        // Look for field start and end
        if (csv->cursor >= csv->datalen) csv_feed(csv);

        if (csv->buffer[csv->cursor] == '"') {
            quoted = true;
            csv->cursor ++;
        }

        unsigned start = csv->cursor;
        if (quoted) {
            while (1) {
                if (0 != csv_find(csv, "\"")) {
                    fprintf(stderr, "No terminating quote\n");
                    return -1;
                }
                if (csv->buffer[csv->cursor+1] == '"') {  // a quoted quote
                    csv->cursor += 2;
                } else if (csv->buffer[csv->cursor+1] != csv->delimiter && csv->buffer[csv->cursor+1] != '\n') {
                    fprintf(stderr, "Unquoted quote in quoted field\n");
                    return -1;
                } else break;
            }
        } else {    // unquoted
            // Check that no quotes are present in the field (by adding quote to any_delimiter?)
            if (0 != csv_find(csv, any_delimiter)) {    // assuming the file is properly terminated by '\n'...
                fprintf(stderr, "Line too long (%u)\n", lineno);
                return -1;
            }
        }

        char tmp = csv->buffer[csv->cursor];
        csv->buffer[csv->cursor] = '\0';
        field_cb(csv->buffer + start, csv->cursor - start, csv->user_data);
        csv->buffer[csv->cursor] = tmp;
        fieldno ++;

        if (quoted) {
            assert(csv->buffer[csv->cursor] == '"');
            csv->cursor ++;
        }
        if (csv->buffer[csv->cursor] == '\n') {
            record_cb(csv->user_data);
            lineno ++;
            fieldno = 0;
            csv->cursor ++;
            csv->upto = csv->cursor;
            if (debug) fprintf(stderr, "eol, cursor=%u, datalen=%u, upto=%u\n", csv->cursor, csv->datalen, csv->upto);
            if (csv->datalen < csv->max_row_size || csv->upto > csv->datalen - csv->max_row_size) {
                csv_feed(csv);
            }
        } else {
            assert(csv->buffer[csv->cursor] == csv->delimiter);
            csv->cursor ++;
        }
    }

    return csv->eof ? 0 : -1;
}
