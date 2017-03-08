#!/usr/bin/env bash
wget http://nlp.cs.illinois.edu/HockenmaierGroup/Framing_Image_Description/Flickr8k_Dataset.zip
unzip Flickr8k_Dataset.zip

# Because whoever packaged this did weird things, we clean this up:
mv Flicker8k_Dataset Flickr8k_Dataset
rm -r __MACOSX/

# Get the data on hdfs
tar -cvf flickr_jpgs.tar Flickr8k_Dataset
~/hadoop/bin/hdfs dfs -mkdir /flickr_jpgs
~/hadoop/bin/hdfs dfs -copyFromLocal flickr_jpgs.tar /flickr_jpgs/flickr_jpgs_1.tar