#!/bin/bash
set -e

NUM_PARTS=32
KEYSTONE_MEM=20g

declare -a DISTRIBUTED_SETTINGS=("" "--communicationRate 0s" "--disableMulticore")
PATCH_SETTINGS="3,2 4,3"
CROP_SETTINGS="0.6,0.6,0.7,0.7"
CONSTANT_POLICIES="constant:0 constant:1 constant:2"
DYNAMIC_POLICIES="epsilon-greedy gaussian-thompson-sampling ucb1 linear-thompson-sampling lin-ucb"

# Execute the trials
for CROP_SETTING in $CROP_SETTINGS
do
    for PATCH_SETTING in $PATCH_SETTINGS
    do
        for POLICY in $CONSTANT_POLICIES
        do
            OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING.csv"
            flintrock run-command --master-only bandits-cluster "
cd keystone-example
KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
  keystoneml.pipelines.ConvolveFlickrData \
  --trainLocation /flickr_jpgs \
  --patchesLocation patches \
  --outputLocation $OUT_CSV \
  --labelLocation flickr_file_shuffled_indexmap.out \
  --policy $POLICY \
  --patches $PATCH_SETTING \
  --crops $CROP_SETTING \
  --numParts $NUM_PARTS
"
            flintrock download-file bandits-cluster keystone-example/$OUT_CSV experiment-results/$OUT_CSV
        done
        for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
        do
            for POLICY in $DYNAMIC_POLICIES
            do
                OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX.csv"
                flintrock run-command --master-only bandits-cluster "
cd keystone-example
KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
  keystoneml.pipelines.ConvolveFlickrData \
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

# Plot all the figures
for CROP_SETTING in $CROP_SETTINGS
do
    for PATCH_SETTING in $PATCH_SETTINGS
    do
        CONSTANT_GLOM="constant:*-$PATCH_SETTING-$CROP_SETTING.csv"
        IDEAL_CSV="experiment-results/ideal-$PATCH_SETTING-$CROP_SETTING.csv"
        python3 scripts/gen_ideal_data.py "experiment-results/$CONSTANT_GLOM" $IDEAL_CSV
        for POLICY in $CONSTANT_POLICIES
        do
            DATA_PATH="experiment-results/$POLICY-$PATCH_SETTING-$CROP_SETTING.csv"
            OUTPUT_FIGURE="$POLICY-$PATCH_SETTING-$CROP_SETTING.png"
            echo "Generating $OUTPUT_FIGURE"
            python3 scripts/gen_plot.py $DATA_PATH $IDEAL_CSV figures/$OUTPUT_FIGURE
        done
        for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
        do
            for POLICY in $DYNAMIC_POLICIES
            do
                DATA_PATH="experiment-results/$POLICY-$PATCH_SETTING-$CROP_SETTING-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX.csv"
                OUTPUT_FIGURE="$POLICY-$PATCH_SETTING-$CROP_SETTING-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX.csv.png"
                echo "Generating $OUTPUT_FIGURE"
                python3 scripts/gen_plot.py $DATA_PATH $IDEAL_CSV figures/$OUTPUT_FIGURE
            done
        done
    done
done
