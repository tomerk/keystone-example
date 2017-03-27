#!/bin/bash
NUM_PARTS=32 #32
KEYSTONE_MEM=20g

declare -a DISTRIBUTED_SETTINGS=("" "--communicationRate 0s" "--disableMulticore")
declare -a WARMUP_SETTINGS=("" "--warmup 5")
PATCH_SETTINGS="5,3:5,20:8,30:24,5:25,1"
CROP_SETTINGS="0,0,0.5,0.5:0,0.5,0.5,1.0:0.5,0,1,0.5:0.5,0.5,1,1"
CONSTANT_POLICIES="constant:0 constant:1 constant:2"
DYNAMIC_POLICIES="kernel-linear-thompson-sampling kernel-lin-ucb epsilon-greedy gaussian-thompson-sampling pseudo-ucb linear-thompson-sampling lin-ucb"

# Execute the trials
for CROP_SETTING in $CROP_SETTINGS
do
    for PATCH_SETTING in $PATCH_SETTINGS
    do
        for POLICY in $CONSTANT_POLICIES
        do
            OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING.csv"
            echo "Generating $OUT_CSV"
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
            for WARMUP_INDEX in "${!WARMUP_SETTINGS[@]}"
            do
                for POLICY in $DYNAMIC_POLICIES
                do
                    OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX.csv"
                    echo "Generating $OUT_CSV"
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
      --numParts $NUM_PARTS ${DISTRIBUTED_SETTINGS[$DISTRIBUTED_SETTING_INDEX]} ${WARMUP_SETTINGS[$WARMUP_INDEX]}
    "
                    flintrock download-file bandits-cluster keystone-example/$OUT_CSV experiment-results/$OUT_CSV

                    # Delete excessive JARs that get copied to each app and fill up disks
                    flintrock run-command bandits-cluster "rm spark/work/*/*/*.jar" > /dev/null
                done
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
            OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY.png"
            echo "Generating $OUTPUT_FIGURE"
            python3 scripts/gen_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" figures/$OUTPUT_FIGURE
            OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-rate.png"
            echo "Generating $OUTPUT_FIGURE"
            python3 scripts/gen_rate_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" figures/$OUTPUT_FIGURE
        done
        for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
        do
            for WARMUP_INDEX in "${!WARMUP_SETTINGS[@]}"
            do
                for POLICY in $DYNAMIC_POLICIES
                do
                    DATA_PATH="experiment-results/$POLICY-$PATCH_SETTING-$CROP_SETTING-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX.csv"
                    OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX.png"
                    echo "Generating $OUTPUT_FIGURE"
                    python3 scripts/gen_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" figures/$OUTPUT_FIGURE
                    OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-rate.png"
                    echo "Generating $OUTPUT_FIGURE"
                    python3 scripts/gen_rate_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" figures/$OUTPUT_FIGURE
                done
            done
        done
    done
done
