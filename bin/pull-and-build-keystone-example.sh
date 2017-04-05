#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"

flintrock run-command $BANDITS_CLUSTER "
cd keystone-example
git pull
sbt/sbt assembly
"
