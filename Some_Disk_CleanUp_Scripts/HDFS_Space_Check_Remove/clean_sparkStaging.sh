#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_HOME=/usr/sdp/current/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

echo "$(date +%Y-%m-%d\ %H:%M:%S)"

hdfs dfs -ls /user/hadoop/.sparkStaging | grep '^d' | awk '{print $8}' | while read dir; do
    modify_time_ms=$(hdfs dfs -stat '%Y' "$dir")
    modify_time=$((modify_time_ms / 1000))
    cutoff_time=$(date -d '3 days ago' +%s)
    if [[ $modify_time -lt $cutoff_time ]]; then
        echo "Directory to be deleted: $dir"
        hdfs dfs -rm -r -skipTrash "$dir"
    fi
done