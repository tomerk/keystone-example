#!/usr/bin/env bash

# Given a list of csv's each w/ a header line, joins them
# Into a single file that has only one header line.
head -n +1 $1
for file in $*; do
    tail -n +2 $file
done
