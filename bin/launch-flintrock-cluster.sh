#!/bin/bash

#if [ -z "$SPARK_EC2_DIR" ] || [ ! -f "$SPARK_EC2_DIR"/spark-ec2 ]; then
#  echo "SPARK_EC2_DIR is not set correctly, please set SPARK_EC2_DIR to be <your_spark_clone>/ec2"
#  exit 1
#fi

BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"
CLUSTER_SIZE="${CLUSTER_SIZE:-8}"

flintrock launch $BANDITS_CLUSTER \
    --spark-version "" \
    --spark-git-repository https://github.com/tomerk/spark.git \
    --spark-git-commit f121b48128f228f50a0f697455ca8526ab2ffd92 \
    --ec2-instance-type r3.xlarge \
    --ec2-spot-price 0.50 \
    --num-slaves $CLUSTER_SIZE

flintrock run-command $BANDITS_CLUSTER 'sudo yum update -y'

flintrock run-command $BANDITS_CLUSTER 'sudo yum install -y git'

flintrock run-command $BANDITS_CLUSTER 'sudo yum install -y tmux'

flintrock run-command $BANDITS_CLUSTER 'sudo yum install -y gcc'

flintrock run-command $BANDITS_CLUSTER 'echo "set-window-option -g mode-keys vi" > .tmux.conf'

flintrock run-command $BANDITS_CLUSTER "
git clone -b bandit-experiments https://github.com/tomerk/keystone-example.git
cd keystone-example
sbt/sbt assembly
"
