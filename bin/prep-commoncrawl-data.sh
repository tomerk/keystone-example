#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"

# Install the AWS credentials for reading from s3
flintrock run-command $BANDITS_CLUSTER "
sed -i'f' 's,.*</configuration>.*,<property><name>fs.s3n.awsAccessKeyId</name><value>$AWS_ACCESS_KEY_ID</value></property><property><name>fs.s3n.awsSecretAccessKey</name><value>$AWS_SECRET_ACCESS_KEY</value></property>&,' ~/hadoop/conf/core-site.xml
"

flintrock run-command --master-only $BANDITS_CLUSTER "./keystone-example/bin/on-cluster/prep-commoncrawl-data.sh"
