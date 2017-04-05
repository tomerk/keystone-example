#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"

flintrock run-command --master-only $BANDITS_CLUSTER "./keystone-example/bin/on-cluster/prep-data.sh"
