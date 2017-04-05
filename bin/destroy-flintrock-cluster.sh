#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"
echo 'y' | flintrock destroy $BANDITS_CLUSTER