#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"
NUM_PARTS=512
KEYSTONE_MEM=20g
SCALE_FACTORS="200"

# Install the tpcds data gen kit
flintrock run-command $BANDITS_CLUSTER "
cd ~
git clone https://github.com/davies/tpcds-kit.git

cd tpcds-kit

# check out the last git commit at the time I used this (in case it changes)
git checkout 39a63a4fa8cc349dc033b990c4ae36fad9110b1b

cd tools
mv Makefile.suite Makefile
make dsdgen
"

#flintrock run-command --master-only $BANDITS_CLUSTER "./keystone-example/bin/on-cluster/prep-tpcds-data.sh"

for SCALE_FACTOR in $SCALE_FACTORS
do
flintrock run-command --master-only $BANDITS_CLUSTER "
cd keystone-example
KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
  keystoneml.pipelines.tpcds.TPCDSDataGen \
  --dsdgenLocation /home/ec2-user/tpcds-kit/tools \
  --outputLocation /tpcds/$SCALE_FACTOR \
  --numParts $NUM_PARTS \
  --scaleFactor $SCALE_FACTOR
"
done
