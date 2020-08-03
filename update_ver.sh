#!/bin/sh

# This script simplifies the process of incrementing all version numbers for a new release.

# make distclean *before* updating the version to make sure all the old version built files are gone.
make distclean

NEWVER="2.14.0"

perl -p -i -e "s/^(AC_INIT\\(\\[[a-z_]+\\],) \\[[0-9.]+\\]/\\1 \\[$NEWVER\\]/" configure.in

perl -p -i -e "s/^(\*Version) [0-9.]+(\*)/\\1 $NEWVER\\2/" README.md

