#!/bin/bash

#if [ -z "$SPARK_EC2_DIR" ] || [ ! -f "$SPARK_EC2_DIR"/spark-ec2 ]; then
#  echo "SPARK_EC2_DIR is not set correctly, please set SPARK_EC2_DIR to be <your_spark_clone>/ec2"
#  exit 1
#fi

flintrock launch bandits-cluster \
    --spark-version "" \
    --spark-git-repository https://github.com/tomerk/spark.git \
    --spark-git-commit cb84b4a3652f46685aff561fee81a44e02bf790b \
    --ec2-instance-type r3.xlarge \
    --ec2-spot-price 1.00 \
    --num-slaves 1

flintrock run-command bandits-cluster 'sudo yum update -y'

flintrock run-command bandits-cluster 'sudo yum install -y git'

flintrock run-command bandits-cluster 'sudo yum install -y tmux'

flintrock run-command bandits-cluster 'echo "set-window-option -g mode-keys vi" > .tmux.conf'
