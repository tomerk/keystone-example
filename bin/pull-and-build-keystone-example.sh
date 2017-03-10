#!/bin/bash

flintrock run-command bandits-cluster "
cd keystone-example
git pull
sbt/sbt assembly
"
