#!/bin/bash

#if [ -z "$SPARK_EC2_DIR" ] || [ ! -f "$SPARK_EC2_DIR"/spark-ec2 ]; then
#  echo "SPARK_EC2_DIR is not set correctly, please set SPARK_EC2_DIR to be <your_spark_clone>/ec2"
#  exit 1
#fi

flintrock launch bandits-cluster \
    --spark-version "" \
    --spark-git-repository https://github.com/tomerk/spark.git \
    --spark-git-commit d2c4c7e6a20bcd9af74fe3b9b95d9eece59778d9 \
    --ec2-instance-type r3.2xlarge \
    --ec2-spot-price 1.00 \
    --num-slaves 1

flintrock run-command bandits-cluster 'sudo yum update -y'

flintrock run-command bandits-cluster 'sudo yum install -y git'

flintrock run-command bandits-cluster 'sudo yum install -y tmux'

flintrock run-command bandits-cluster 'echo "set-window-option -g mode-keys vi" > .tmux.conf'

flintrock run-command bandits-cluster "
git clone -b bandit-experiments https://github.com/tomerk/keystone-example.git
cd keystone-example
sbt/sbt assembly
"
