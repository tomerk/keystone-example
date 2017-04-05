#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "Launching cluster"
$DIR/launch-flintrock-cluster.sh

flintrock describe $BANDITS_CLUSTER

echo "Prepping data"
$DIR/prep-flickr-data.sh

echo "Running experiments"
$DIR/run-flickr-experiments.sh

echo "Destroying Cluster"
$DIR/destroy-flintrock-cluster.sh