#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"
NUM_PARTS=32 #32
KEYSTONE_MEM=20g

#declare -a DISTRIBUTED_SETTINGS=("" "--communicationRate 0s" "--disableMulticore")
declare -a DISTRIBUTED_SETTINGS=("")
#declare -a WARMUP_SETTINGS=("" "--warmup 5")
declare -a WARMUP_SETTINGS=("--warmup 5")
NONSTATIONARY_SETTINGS="stationary sort_partitions sort random_walk,0.05 global_random_walk,0.05" #("--nonstationarity sort_partitions" "--nonstationarity global_random_walk,0.05") #"--nonstationarity sort" "--nonstationarity random_walk,0.05" "--nonstationarity periodic") #NONSTATIONARY_SETTINGS=("" "--nonstationarity sort" "--nonstationarity periodic" "--nonstationarity sort_partition" "--nonstationarity random_walk,0.05" )
CLUSTER_COEFFICIENT_SETTINGS="10000 1.0 0.5 0.1 0" #"5,3 5,3:5,20:8,30:24,5:25,1"

PATCH_SETTINGS="5,3:5,20:8,30:24,5:25,1" #"5,3 5,3:5,20:8,30:24,5:25,1"
CROP_SETTINGS="0,0,0.5,0.5" #"0,0,0.5,0.5:0,0.5,0.5,1.0:0.5,0,1,0.5:0.5,0.5,1,1"
CONSTANT_POLICIES="constant:0 constant:1 constant:2"
ORACLE_POLICIES="oracle:min"
DYNAMIC_POLICIES="ucb1-normal:0.4 ucb-gaussian-bayes lin-ucb:image_rows,image_cols,filter_rows,filter_cols,image_size,fft_cost_model,matrix_multiply_cost_model" #"pseudo-ucb gaussian-thompson-sampling lin-ucb:bias,fft_cost_model,matrix_multiply_cost_model,pos_in_partition,global_index,periodic_5" #epsilon-greedy gaussian-thompson-sampling pseudo-ucb linear-thompson-sampling lin-ucb"

# Execute the trials
for CROP_SETTING in $CROP_SETTINGS
do
    for PATCH_SETTING in $PATCH_SETTINGS
    do
        for NONSTATIONARY_SETTING in $NONSTATIONARY_SETTINGS
        do
            for POLICY in $CONSTANT_POLICIES
            do
                OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
                echo "Generating $OUT_CSV"
                flintrock run-command --master-only $BANDITS_CLUSTER "
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
      --numParts $NUM_PARTS --warmup 5 --nonstationarity $NONSTATIONARY_SETTING
    "
                flintrock download-file $BANDITS_CLUSTER keystone-example/$OUT_CSV experiment-results/$OUT_CSV

                # Delete excessive JARs that get copied to each app and fill up disks
                flintrock run-command $BANDITS_CLUSTER "rm spark/work/*/*/*.jar" > /dev/null
            done

            CONSTANT_GLOM="constant:*-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
            flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    cat $CONSTANT_GLOM > oracle_data.csv
    "
            for POLICY in $ORACLE_POLICIES
            do
                OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
                echo "Generating $OUT_CSV"
                flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
      keystoneml.pipelines.ConvolveFlickrData \
      --trainLocation /flickr_jpgs \
      --patchesLocation patches \
      --outputLocation $OUT_CSV \
      --labelLocation flickr_file_shuffled_indexmap.out \
      --policy $POLICY:oracle_data.csv \
      --patches $PATCH_SETTING \
      --crops $CROP_SETTING \
      --numParts $NUM_PARTS --warmup 5 --nonstationarity $NONSTATIONARY_SETTING
    "
                flintrock download-file $BANDITS_CLUSTER keystone-example/$OUT_CSV experiment-results/$OUT_CSV

                # Delete excessive JARs that get copied to each app and fill up disks
                flintrock run-command $BANDITS_CLUSTER "rm spark/work/*/*/*.jar" > /dev/null
            done

            for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
            do
                for CLUSTER_COEFFICIENT_SETTING in $CLUSTER_COEFFICIENT_SETTINGS
                do
                    for WARMUP_INDEX in "${!WARMUP_SETTINGS[@]}"
                    do
                        for POLICY in $DYNAMIC_POLICIES
                        do
                            OUT_CSV="$POLICY-$PATCH_SETTING-$CROP_SETTING-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING.csv"
                            echo "Generating $OUT_CSV"
                            flintrock run-command --master-only $BANDITS_CLUSTER "
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
              --numParts $NUM_PARTS \
              ${DISTRIBUTED_SETTINGS[$DISTRIBUTED_SETTING_INDEX]} \
              ${WARMUP_SETTINGS[$WARMUP_INDEX]} \
              --nonstationarity $NONSTATIONARY_SETTING \
              --clusterCoefficient $CLUSTER_COEFFICIENT_SETTING
            "
                            flintrock download-file $BANDITS_CLUSTER keystone-example/$OUT_CSV experiment-results/$OUT_CSV

                            # Delete excessive JARs that get copied to each app and fill up disks
                            flintrock run-command $BANDITS_CLUSTER "rm spark/work/*/*/*.jar" > /dev/null
                        done
                    done
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
        for NONSTATIONARY_SETTING in $NONSTATIONARY_SETTINGS
        do
            CONSTANT_GLOM="constant:*-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
            ORACLE_GLOM="oracle:*-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
            for POLICY in $CONSTANT_POLICIES
            do
                DATA_PATH="experiment-results/$POLICY-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
                OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-$NONSTATIONARY_SETTING.png"
                echo "Generating $OUTPUT_FIGURE"
                python3 scripts/gen_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
                OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-$NONSTATIONARY_SETTING-rate.png"
                echo "Generating $OUTPUT_FIGURE"
                python3 scripts/gen_rate_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
            done
            for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
            do
                for CLUSTER_COEFFICIENT_SETTING in $CLUSTER_COEFFICIENT_SETTINGS
                do
                    for WARMUP_INDEX in "${!WARMUP_SETTINGS[@]}"
                    do
                        for POLICY in $DYNAMIC_POLICIES
                        do
                            DATA_PATH="experiment-results/$POLICY-$PATCH_SETTING-$CROP_SETTING-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING.csv"
                            OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING.png"
                            echo "Generating $OUTPUT_FIGURE"
                            python3 scripts/gen_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
                            OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING-rate.png"
                            echo "Generating $OUTPUT_FIGURE"
                            python3 scripts/gen_rate_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
                        done
                    done
                done
            done
        done
    done
done
