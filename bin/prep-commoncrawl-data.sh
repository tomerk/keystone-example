#!/bin/bash
# Install the AWS credentials for reading from s3
flintrock run-command bandits-cluster "
sed -i'f' 's,.*</configuration>.*,<property><name>fs.s3n.awsAccessKeyId</name><value>$AWS_ACCESS_KEY_ID</value></property><property><name>fs.s3n.awsSecretAccessKey</name><value>$AWS_SECRET_ACCESS_KEY</value></property>&,' ~/hadoop/conf/core-site.xml
"

flintrock run-command --master-only bandits-cluster "./keystone-example/bin/on-cluster/prep-commoncrawl-data.sh"
