#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([groupby], [0.1], [cedric.cellier@securactive.net])
AC_CONFIG_SRCDIR([groupby.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_LN_S

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([stdint.h stdlib.h string.h strings.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([strcasecmp strchr strerror strtoul])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
