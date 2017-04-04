#!/bin/bash
# Install the tpcds data gen kit
flintrock run-command bandits-cluster "
cd ~
git clone https://github.com/davies/tpcds-kit.git

cd tpcds-kit

# check out the last git commit at the time I used this (in case it changes)
git checkout 39a63a4fa8cc349dc033b990c4ae36fad9110b1b

cd tools
mv Makefile.suite Makefile
make dsdgen
"

flintrock run-command --master-only bandits-cluster "./keystone-example/bin/on-cluster/prep-tpcds-data.sh"
