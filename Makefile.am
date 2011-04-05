AUTOMAKE_OPTIONS = foreign

AM_CFLAGS = -std=c99 -Wall -W
AM_CPPFLAGS = -D_GNU_SOURCE

bin_PROGRAMS = groupby

groupby_SOURCES = main.c aggr.c groupby.h groupby.c

.PHONY: cscope clear

cscope:
	cd $(top_srcdir) && cscope -Rb $(CPPFLAGS)

distclean-local:
	rm -f cscope.out

clear:
	find $(top_srcdir) -type f -\( -name '*.c' -o -name '*.h' -\) | xargs sed -i -e 's/[ \t]\+$$//'

