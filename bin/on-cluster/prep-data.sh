#!/usr/bin/env bash
wget http://nlp.cs.illinois.edu/HockenmaierGroup/Framing_Image_Description/Flickr8k_Dataset.zip
unzip Flickr8k_Dataset.zip

# Because whoever packaged this did weird things, we clean this up:
mv Flicker8k_Dataset Flickr8k_Dataset
rm -r __MACOSX/
cd Flicker8k_Dataset

# Split the data into multiple tars, and upload to hdfs.
# Directory splitting code sourced from:
# http://stackoverflow.com/questions/29116212/split-a-folder-into-multiple-subfolders-in-terminal-bash-script
~/hadoop/bin/hdfs dfs -mkdir /flickr_jpgs
dir_size=125
dir_name="flickr_jpgs"
n=$((`find . -maxdepth 1 -type f | wc -l`/$dir_size+1))
for i in `seq 1 $n`;
do
    mkdir -p "$dir_name$i";
    find . -maxdepth 1 -type f | head -n $dir_size | xargs -i mv "{}" "$dir_name$i"
    tar -cvf $dir_name$i.tar $dir_name$i
    ~/hadoop/bin/hdfs dfs -copyFromLocal $dir_name$i.tar /flickr_jpgs/$dir_name$i.tar
done

cd ../