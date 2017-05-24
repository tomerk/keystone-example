#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"
NUM_PARTS=32 #32
KEYSTONE_MEM=20g
WORKLOAD_NAME=commoncrawl-$BANDITS_CLUSTER

#declare -a DISTRIBUTED_SETTINGS=("" "--communicationRate 0s" "--disableMulticore")
declare -a DISTRIBUTED_SETTINGS=("--communicationRate 500ms")
#declare -a WARMUP_SETTINGS=("" "--warmup 5")
declare -a WARMUP_SETTINGS=("--warmup 5")
NONSTATIONARY_SETTINGS="stationary"
CLUSTER_COEFFICIENT_SETTINGS="1.0e10" #"1.0e10 1.0 0.0" #"5,3 5,3:5,20:8,30:24,5:25,1"
DRIFT_COEFFICIENT_SETTINGS="1.0" #"5,3 5,3:5,20:8,30:24,5:25,1"
DRIFT_RATE_SETTINGS="999999s" # 10s" #"999999s 30s 10s 5s" #"5,3 5,3:5,20:8,30:24,5:25,1"

REGEXES="0 1 2"
CONSTANT_POLICIES="constant:0 constant:1 constant:2 constant:3"
ORACLE_POLICIES="oracle:min"
DYNAMIC_POLICIES="ucb1-normal:0.4" #"ucb1-normal:0.4" # lin-ucb:image_rows,image_cols,filter_rows,filter_cols,image_size,fft_cost_model,matrix_multiply_cost_model" # ucb1-normal:0.4 ucb-gaussian-bayes lin-ucb:image_rows,image_cols,filter_rows,filter_cols,image_size,fft_cost_model,matrix_multiply_cost_model" #"pseudo-ucb gaussian-thompson-sampling lin-ucb:bias,fft_cost_model,matrix_multiply_cost_model,pos_in_partition,global_index,periodic_5" #epsilon-greedy gaussian-thompson-sampling pseudo-ucb linear-thompson-sampling lin-ucb"

# Execute the trials
for REGEX_SETTING in $REGEXES
do
        for NONSTATIONARY_SETTING in $NONSTATIONARY_SETTINGS
        do
            for POLICY in $CONSTANT_POLICIES
            do
                OUT_CSV="$WORKLOAD_NAME-$POLICY-$REGEX_SETTING-$NONSTATIONARY_SETTING.csv"
                echo "Generating $OUT_CSV"
                flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
      keystoneml.pipelines.CommonCrawlRegex \
      --trainLocation /common_crawl \
      --outputLocation $OUT_CSV \
      --policy $POLICY \
      --regex $REGEX_SETTING \
      --numParts $NUM_PARTS --warmup 5 --nonstationarity $NONSTATIONARY_SETTING --clusterCoefficient 999981.0 --driftDetectionRate 999981s --driftCoefficient 999981.0
    "
                flintrock download-file $BANDITS_CLUSTER keystone-example/$OUT_CSV experiment-results/$OUT_CSV

                # Delete excessive JARs that get copied to each app and fill up disks
                flintrock run-command $BANDITS_CLUSTER "rm spark/work/*/*/*.jar" > /dev/null
            done

            CONSTANT_GLOM="$WORKLOAD_NAME-constant:*-$REGEX_SETTING-$NONSTATIONARY_SETTING.csv"
            flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    cat $CONSTANT_GLOM > oracle_data.csv
    "
            for POLICY in $ORACLE_POLICIES
            do
                OUT_CSV="$WORKLOAD_NAME-$POLICY-$REGEX_SETTING-$NONSTATIONARY_SETTING.csv"
                echo "Generating $OUT_CSV"
                flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
      keystoneml.pipelines.CommonCrawlRegex \
      --trainLocation /common_crawl \
      --outputLocation $OUT_CSV \
      --policy $POLICY:oracle_data.csv \
      --regex $REGEX_SETTING \
      --numParts $NUM_PARTS --warmup 5 --nonstationarity $NONSTATIONARY_SETTING --clusterCoefficient 999981.0 --driftDetectionRate 999981s --driftCoefficient 999981.0
    "
                flintrock download-file $BANDITS_CLUSTER keystone-example/$OUT_CSV experiment-results/$OUT_CSV

                # Delete excessive JARs that get copied to each app and fill up disks
                flintrock run-command $BANDITS_CLUSTER "rm spark/work/*/*/*.jar" > /dev/null
            done

            for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
            do
                for CLUSTER_COEFFICIENT_SETTING in $CLUSTER_COEFFICIENT_SETTINGS
                do
                    for DRIFT_COEFFICIENT_SETTING in $DRIFT_COEFFICIENT_SETTINGS
                    do
                        for DRIFT_RATE_SETTING in $DRIFT_RATE_SETTINGS
                        do
                            for WARMUP_INDEX in "${!WARMUP_SETTINGS[@]}"
                            do
                                for POLICY in $DYNAMIC_POLICIES
                                do
                                    OUT_CSV="$WORKLOAD_NAME-$POLICY-$REGEX_SETTING-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING-drift-$DRIFT_RATE_SETTING-$DRIFT_COEFFICIENT_SETTING.csv"
                                    echo "Generating $OUT_CSV"
                                    flintrock run-command --master-only $BANDITS_CLUSTER "
                    cd keystone-example
                    KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
                      keystoneml.pipelines.CommonCrawlRegex \
                      --trainLocation /common_crawl \
                      --outputLocation $OUT_CSV \
                      --policy $POLICY \
                      --regex $REGEX_SETTING \
                      --numParts $NUM_PARTS \
                      ${DISTRIBUTED_SETTINGS[$DISTRIBUTED_SETTING_INDEX]} \
                      ${WARMUP_SETTINGS[$WARMUP_INDEX]} \
                      --nonstationarity $NONSTATIONARY_SETTING \
                      --clusterCoefficient $CLUSTER_COEFFICIENT_SETTING --driftDetectionRate $DRIFT_RATE_SETTING --driftCoefficient $DRIFT_COEFFICIENT_SETTING
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
done
#
## Plot all the figures
#for CROP_SETTING in $CROP_SETTINGS
#do
#    for PATCH_SETTING in $PATCH_SETTINGS
#    do
#        for NONSTATIONARY_SETTING in $NONSTATIONARY_SETTINGS
#        do
#            CONSTANT_GLOM="constant:*-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
#            ORACLE_GLOM="oracle:*-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
#            for POLICY in $CONSTANT_POLICIES
#            do
#                DATA_PATH="experiment-results/$POLICY-$PATCH_SETTING-$CROP_SETTING-$NONSTATIONARY_SETTING.csv"
#                OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-$NONSTATIONARY_SETTING.png"
#                echo "Generating $OUTPUT_FIGURE"
#                python3 scripts/gen_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
#                OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-$NONSTATIONARY_SETTING-rate.png"
#                echo "Generating $OUTPUT_FIGURE"
#                python3 scripts/gen_rate_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
#            done
#            for DISTRIBUTED_SETTING_INDEX in "${!DISTRIBUTED_SETTINGS[@]}"
#            do
#                for CLUSTER_COEFFICIENT_SETTING in $CLUSTER_COEFFICIENT_SETTINGS
#                do
#                    for WARMUP_INDEX in "${!WARMUP_SETTINGS[@]}"
#                    do
#                        for POLICY in $DYNAMIC_POLICIES
#                        do
#                            DATA_PATH="experiment-results/$POLICY-$PATCH_SETTING-$CROP_SETTING-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING.csv"
#                            OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING.png"
#                            echo "Generating $OUTPUT_FIGURE"
#                            python3 scripts/gen_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
#                            OUTPUT_FIGURE="$PATCH_SETTING-$CROP_SETTING-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$NONSTATIONARY_SETTING-$CLUSTER_COEFFICIENT_SETTING-rate.png"
#                            echo "Generating $OUTPUT_FIGURE"
#                            python3 scripts/gen_rate_plot.py $DATA_PATH "experiment-results/$CONSTANT_GLOM" "experiment-results/$ORACLE_GLOM" figures/$OUTPUT_FIGURE
#                        done
#                    done
#                done
#            done
#        done
#    done
#done
