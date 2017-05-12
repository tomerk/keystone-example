#!/bin/bash
BANDITS_CLUSTER="${BANDITS_CLUSTER:-bandits-cluster}"
NUM_PARTS=32 #32
KEYSTONE_MEM=20g
WORKLOAD_NAME=tpcds-bandit-q87-q30

#declare -a DISTRIBUTED_SETTINGS=("" "--communicationRate 0s" "--disableMulticore")
declare -a DISTRIBUTED_SETTINGS=("--communicationRate 500ms")
declare -a WARMUP_SETTINGS=("--warmup 5")
CLUSTER_COEFFICIENT_SETTINGS="1.0e10" #"5,3 5,3:5,20:8,30:24,5:25,1"
DRIFT_COEFFICIENT_SETTINGS="1.0" #"5,3 5,3:5,20:8,30:24,5:25,1"
DRIFT_RATE_SETTINGS="999999s" #"5,3 5,3:5,20:8,30:24,5:25,1"
#SPARK_SETTINGS="spark.sql.shuffle.partitions:16,spark.sql.codegen.wholeStage:true-spark.sql.shuffle.partitions:32,spark.sql.codegen.wholeStage:true-spark.sql.shuffle.partitions:16,spark.sql.codegen.wholeStage:false-spark.sql.shuffle.partitions:32,spark.sql.codegen.wholeStage:false"
#SPARK_SETTINGS="spark.io.compression.codec:lz4-spark.io.compression.codec:lzf-spark.io.compression.codec:snappy"
#SPARK_SETTINGS="spark.sql.shuffle.partitions:8-spark.sql.shuffle.partitions:16-spark.sql.shuffle.partitions:24-spark.sql.shuffle.partitions:32-spark.sql.shuffle.partitions:48-spark.sql.shuffle.partitions:64"
SPARK_SETTINGS="spark.sql.shuffle.partitions:512,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:512,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:sort-spark.sql.shuffle.partitions:512,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:hash"
#"spark.sql.shuffle.partitions:256,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:256,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:sort-spark.sql.shuffle.partitions:256,spark.sql.join.banditJoin:false,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:256,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:hash-spark.sql.shuffle.partitions:512,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:512,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:sort-spark.sql.shuffle.partitions:512,spark.sql.join.banditJoin:false,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:512,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:hash-spark.sql.shuffle.partitions:128,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:128,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:sort-spark.sql.shuffle.partitions:128,spark.sql.join.banditJoin:false,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:128,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:hash-spark.sql.shuffle.partitions:64,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:64,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:sort-spark.sql.shuffle.partitions:64,spark.sql.join.banditJoin:false,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:64,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:hash-spark.sql.shuffle.partitions:1024,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:1024,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:sort-spark.sql.shuffle.partitions:1024,spark.sql.join.banditJoin:false,spark.sql.join.bandit.shuffleSortHash:both-spark.sql.shuffle.partitions:1024,spark.sql.join.banditJoin:true,spark.sql.join.bandit.shuffleSortHash:hash"
#QUERIES_SETTINGS="q19:5-q42:5-q52:5-q55:5-q63:5-q68:5-q73:5-q98:5" # The interactive bucket from the impala paper
#QUERIES_SETTINGS="q27:1-q3:1-q43:1-q53:1-q7:1-q89:1" # The reporting bucket
QUERIES_SETTINGS="q87:5-q30:5" #"q1:1-q2:1-q4:1-q5:1-q6:1-q10:1-q11:1-q14a:1-q14b:1-q16:1-q17:1-q24a:1-q24b:1-q25:1-q29:1-q30:1-q31:1-q32:1-q35:1-q37:1-q38:1-q39a:1-q39b:1-q40:1-q47:1-q49:1-q50:1-q54:1-q57:1-q58:1-q59:1-q64:1-q65:1-q72:1-q74:1-q75:1-q78:1-q80:1-q81:1-q82:1-q83:1-q84:1-q85:1-q87:1-q92:1-q93:1-q94:1-q95:1"
#QUERIES_SETTINGS="q19:1-q42:1-q52:1-q55:1-q63:1-q68:1-q73:1-q98:1-q27:1-q3:1-q43:1-q53:1-q7:1-q89:1-q34:1-q46:1-q59:1-q79:1-q49:1-q72:1-q75:1-q78:1-q80:1-q93:1-q23a:1-q23b:1-q24a:1-q24b:1"
#QUERIES_SETTINGS="q19:1-q42:1-q52:1-q55:1-q63:1-q68:1-q73:1-q98:1-q27:1-q3:1-q43:1-q53:1-q7:1-q89:1-q34:1-q46:1-q59:1-q79:1" # The reporting bucket
SCALE_FACTORS="200" # 100 40 20"

CONSTANT_POLICIES="constant:0 constant:1 constant:2" # constant:3 constant:4 constant:5 constant:6 constant:7 constant:8 constant:9 constant:10 constant:11 constant:12 constant:13 constant:14 constant:15 constant:16 constant:17 constant:18 constant:19"
ORACLE_POLICIES="" #"oracle:min"
DYNAMIC_POLICIES="" #"ucb1-normal:0.4" # ucb1-normal:0.4 ucb-gaussian-bayes lin-ucb:image_rows,image_cols,filter_rows,filter_cols,image_size,fft_cost_model,matrix_multiply_cost_model" #"pseudo-ucb gaussian-thompson-sampling lin-ucb:bias,fft_cost_model,matrix_multiply_cost_model,pos_in_partition,global_index,periodic_5" #epsilon-greedy gaussian-thompson-sampling pseudo-ucb linear-thompson-sampling lin-ucb"

# Execute the trials
for SCALE_FACTOR in $SCALE_FACTORS
do
    for QUERIES_SETTING in $QUERIES_SETTINGS
    do
        for SPARK_SETTING in $SPARK_SETTINGS
        do
            for POLICY in $CONSTANT_POLICIES
            do
                OUT_CSV="$WORKLOAD_NAME-$SCALE_FACTOR-$POLICY.csv"
                echo "Generating $OUT_CSV"
                flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
      keystoneml.pipelines.tpcds.TPCDSQueryBenchmark \
      --dataLocation /tpcds/$SCALE_FACTOR \
      --outputLocation $OUT_CSV \
      --policy $POLICY \
      --queries $QUERIES_SETTING \
      --confSettings $SPARK_SETTING --cacheTables False
    "
                flintrock download-file $BANDITS_CLUSTER keystone-example/$OUT_CSV experiment-results/$OUT_CSV

                # Delete excessive JARs that get copied to each app and fill up disks
                flintrock run-command $BANDITS_CLUSTER "rm spark/work/*/*/*.jar" > /dev/null
                flintrock run-command $BANDITS_CLUSTER "for FILE in hadoop/logs/*.out; do : > \$FILE; done;" > /dev/null
            done

            CONSTANT_GLOM="$WORKLOAD_NAME-$SCALE_FACTOR-constant:*.csv"
            flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    cat $CONSTANT_GLOM > oracle_data.csv
    "
            for POLICY in $ORACLE_POLICIES
            do
                OUT_CSV="$WORKLOAD_NAME-$SCALE_FACTOR-$POLICY.csv"
                echo "Generating $OUT_CSV"
                flintrock run-command --master-only $BANDITS_CLUSTER "
    cd keystone-example
    KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
      keystoneml.pipelines.tpcds.TPCDSQueryBenchmark \
      --dataLocation /tpcds/$SCALE_FACTOR \
      --outputLocation $OUT_CSV \
      --policy $POLICY:oracle_data.csv \
      --queries $QUERIES_SETTING \
      --confSettings $SPARK_SETTING --cacheTables False
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
                                    OUT_CSV="$WORKLOAD_NAME-$SCALE_FACTOR-$POLICY-WARMUP_$WARMUP_INDEX-DISTRIBUTED_$DISTRIBUTED_SETTING_INDEX-$CLUSTER_COEFFICIENT_SETTING-drift-$DRIFT_RATE_SETTING-$DRIFT_COEFFICIENT_SETTING.csv"
                                    echo "Generating $OUT_CSV"
                                    flintrock run-command --master-only $BANDITS_CLUSTER "
                    cd keystone-example
                    KEYSTONE_MEM=$KEYSTONE_MEM ./bin/run-pipeline.sh \
                      keystoneml.pipelines.tpcds.TPCDSQueryBenchmark \
                      --dataLocation /tpcds/$SCALE_FACTOR \
                      --outputLocation $OUT_CSV \
                      --policy $POLICY \
                        --queries $QUERIES_SETTING \
                        --confSettings $SPARK_SETTING \
                      ${DISTRIBUTED_SETTINGS[$DISTRIBUTED_SETTING_INDEX]} \
                      ${WARMUP_SETTINGS[$WARMUP_INDEX]} \
                      --clusterCoefficient $CLUSTER_COEFFICIENT_SETTING --driftDetectionRate $DRIFT_RATE_SETTING --driftCoefficient $DRIFT_COEFFICIENT_SETTING --cacheTables False
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
done