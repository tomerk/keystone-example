#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"
NUM_PARTS=32 #32
KEYSTONE_MEM=20g
WORKLOAD_NAME=tpcds-bandit-join

#declare -a DISTRIBUTED_SETTINGS=("" "--communicationRate 0s" "--disableMulticore")
declare -a DISTRIBUTED_SETTINGS=("--communicationRate 500ms")
declare -a WARMUP_SETTINGS=("--warmup 5")
CLUSTER_COEFFICIENT_SETTINGS="1.0e10" #"5,3 5,3:5,20:8,30:24,5:25,1"
DRIFT_COEFFICIENT_SETTINGS="1.0" #"5,3 5,3:5,20:8,30:24,5:25,1"
DRIFT_RATE_SETTINGS="999999s" #"5,3 5,3:5,20:8,30:24,5:25,1"
#SPARK_SETTINGS="spark.sql.shuffle.partitions:16,spark.sql.codegen.wholeStage:true-spark.sql.shuffle.partitions:32,spark.sql.codegen.wholeStage:true-spark.sql.shuffle.partitions:16,spark.sql.codegen.wholeStage:false-spark.sql.shuffle.partitions:32,spark.sql.codegen.wholeStage:false"
#SPARK_SETTINGS="spark.io.compression.codec:lz4-spark.io.compression.codec:lzf-spark.io.compression.codec:snappy"
#SPARK_SETTINGS="spark.sql.shuffle.partitions:8-spark.sql.shuffle.partitions:16-spark.sql.shuffle.partitions:24-spark.sql.shuffle.partitions:32-spark.sql.shuffle.partitions:48-spark.sql.shuffle.partitions:64"
SPARK_SETTINGS="spark.sql.shuffle.partitions:128,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:true,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:128,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:128,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:false-spark.sql.shuffle.partitions:256,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:true,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:256,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:256,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:false-spark.sql.shuffle.partitions:512,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:true,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:512,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:512,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:false-spark.sql.shuffle.partitions:1024,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:true,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:1024,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:true-spark.sql.shuffle.partitions:1024,spark.bandits.driftDetectionRate:99999999s,spark.bandits.alwaysShare:true,spark.bandits.communicationRate:500ms,spark.sql.join.banditJoin:false,spark.sql.join.preferSortMergeJoin:false"
#QUERIES_SETTINGS="q19:5-q42:5-q52:5-q55:5-q63:5-q68:5-q73:5-q98:5" # The interactive bucket from the impala paper
#QUERIES_SETTINGS="q27:1-q3:1-q43:1-q53:1-q7:1-q89:1" # The reporting bucket
QUERIES_SETTINGS="q49:3-q72:3-q75:3-q78:3-q80:3-q93:3"
#QUERIES_SETTINGS="q19:1-q42:1-q52:1-q55:1-q63:1-q68:1-q73:1-q98:1-q27:1-q3:1-q43:1-q53:1-q7:1-q89:1-q34:1-q46:1-q59:1-q79:1-q49:1-q72:1-q75:1-q78:1-q80:1-q93:1-q23a:1-q23b:1-q24a:1-q24b:1"
#QUERIES_SETTINGS="q19:1-q42:1-q52:1-q55:1-q63:1-q68:1-q73:1-q98:1-q27:1-q3:1-q43:1-q53:1-q7:1-q89:1-q34:1-q46:1-q59:1-q79:1" # The reporting bucket

CONSTANT_POLICIES="constant:7 constant:8 constant:9 constant:10 constant:11"
ORACLE_POLICIES="" #"oracle:min"
DYNAMIC_POLICIES="" #"ucb1-normal:0.4" # ucb1-normal:0.4 ucb-gaussian-bayes lin-ucb:image_rows,image_cols,filter_rows,filter_cols,image_size,fft_cost_model,matrix_multiply_cost_model" #"pseudo-ucb gaussian-thompson-sampling lin-ucb:bias,fft_cost_model,matrix_multiply_cost_model,pos_in_partition,global_index,periodic_5" #epsilon-greedy gaussian-thompson-sampling pseudo-ucb linear-thompson-sampling lin-ucb"

# Execute the trials
for QUERIES_SETTING in $QUERIES_SETTINGS
do
    for SPARK_SETTING in $SPARK_SETTINGS
    do
        for POLICY in $CONSTANT_POLICIES
        do
            OUT_CSV="$WORKLOAD_NAME-$POLICY.csv"
            echo "Generating $OUT_CSV"
            flintrock run-command --master-only $BANDITS_CLUSTER "
cd keystone-example
KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
  keystoneml.pipelines.tpcds.TPCDSQueryBenchmark \
  --dataLocation /tpcds \
  --outputLocation $OUT_CSV \
  --policy $POLICY \
  --queries $QUERIES_SETTING \
  --confSettings $SPARK_SETTING
"
            flintrock download-file $BANDITS_CLUSTER keystone-example/$OUT_CSV experiment-results/$OUT_CSV

            # Delete excessive JARs that get copied to each app and fill up disks
            flintrock run-command $BANDITS_CLUSTER "rm spark/work/*/*/*.jar" > /dev/null
            flintrock run-command $BANDITS_CLUSTER "for FILE in hadoop/logs/*.out; do : > \$FILE; done;" > /dev/null
        done

        CONSTANT_GLOM="$WORKLOAD_NAME-constant:*.csv"
        flintrock run-command --master-only $BANDITS_CLUSTER "
cd keystone-example
cat $CONSTANT_GLOM > oracle_data.csv
"
        for POLICY in $ORACLE_POLICIES
        do
            OUT_CSV="$WORKLOAD_NAME-$POLICY.csv"
            echo "Generating $OUT_CSV"
            flintrock run-command --master-only $BANDITS_CLUSTER "
cd keystone-example
KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
  keystoneml.pipelines.tpcds.TPCDSQueryBenchmark \
  --dataLocation /tpcds \
  --outputLocation $OUT_CSV \
  --policy $POLICY:oracle_data.csv \
  --queries $QUERIES_SETTING \
  --confSettings $SPARK_SETTING
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
                                OUT_CSV="$WORKLOAD_NAME-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$CLUSTER_COEFFICIENT_SETTING-drift-$DRIFT_RATE_SETTING-$DRIFT_COEFFICIENT_SETTING.csv"
                                echo "Generating $OUT_CSV"
                                flintrock run-command --master-only $BANDITS_CLUSTER "
                cd keystone-example
                KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
                  keystoneml.pipelines.tpcds.TPCDSQueryBenchmark \
                  --dataLocation /tpcds \
                  --outputLocation $OUT_CSV \
                  --policy $POLICY \
                    --queries $QUERIES_SETTING \
                    --confSettings $SPARK_SETTING \
                  ${DISTRIBUTED_SETTINGS[$DISTRIBUTED_SETTING_INDEX]} \
                  ${WARMUP_SETTINGS[$WARMUP_INDEX]} \
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
