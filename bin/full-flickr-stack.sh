#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "Launching cluster"
$DIR/launch-flintrock-cluster.sh

echo "Prepping data"
$DIR/launch-flintrock-cluster.sh

echo "Running experiments"
$DIR/run-flickr-experiments.sh

echo "Destroying Cluster"
$DIR/destroy-flintrock-cluster.sh