#!/usr/bin/env bash
# This script requires aws credentials to be set
for id in `echo {00001..32}`;
do
    echo "s3n://commoncrawl/crawl-data/CC-MAIN-2014-10/segments/1394023864559/warc/CC-MAIN-20140305125104-$id-ip-10-183-142-35.ec2.internal.warc.gz" >> commoncrawl_file_list.txt
done

# Download the appropriate files
~/hadoop/bin/hdfs dfs -mkdir /common_crawl
~/hadoop/bin/hdfs dfs -copyFromLocal commoncrawl_file_list.txt /
~/hadoop/bin/hadoop distcp -f /commoncrawl_file_list.txt /common_crawl
