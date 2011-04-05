#!/bin/sh
# Usage: sh -x ./autogen.sh

set -e

mkdir -p build-aux &&
aclocal &&
autoheader &&
automake --add-missing --foreign &&
autoconf &&
echo "Now run configure and make."
