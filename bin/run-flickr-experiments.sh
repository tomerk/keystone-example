#!/bin/bash
set -e

NUM_PARTS=32
KEYSTONE_MEM=20g

declare -a DISTRIBUTED_SETTINGS=("" "--communicationRate 0s" "--disableMulticore")
PATCH_SETTINGS="3,2 4,3"
CROP_SETTINGS="0.6,0.6,0.7,0.7"
POLICIES="constant:0 constant:1 constant:2 epsilon-greedy gaussian-thompson-sampling ucb1 linear-thompson-sampling lin-ucb"

# Execute the non-exact trials
for CROP_SETTING in $CROP_SETTINGS
do
    for PATCH_SETTING in $PATCH_SETTINGS
    do
        for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
        do
            for POLICY in $POLICIES
            do
                OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX.csv"
                flintrock run-command bandits-cluster "
cd keystone-example
KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
  keystoneml.pipelines.PrepFlickrData \
  --trainLocation /flickr_jpgs \
  --patchesLocation patches \
  --outputLocation $OUT_CSV \
  --labelLocation flickr_file_shuffled_indexmap.out \
  --policy $POLICY \
  --patches $PATCH_SETTING \
  --crops $CROP_SETTING \
  --numParts $NUM_PARTS ${DISTRIBUTED_SETTINGS[$DISTRIBUTED_SETTING_INDEX]}
"
                flintrock download-file bandits-cluster keystone-example/$OUT_CSV experiment-results/$OUT_CSV
            done
        done
    done
done

