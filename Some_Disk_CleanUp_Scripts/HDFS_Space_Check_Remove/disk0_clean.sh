#!/bin/bash
# For cleaning /data/disk0 on prod-ailab-sort-minute-l-001, keep rencent 3 days of files.

#disk_usage=$(df -h /data/disk0 | awk 'NR==2 {print int($5)}')

path="/data/disk0/hadoop/yarn/local/usercache/hadoop/filecache"
echo "$(date +%Y-%m-%d\ %H:%M:%S)"
find "$path" -mindepth 1 -maxdepth 1 -type d -mtime +3 -exec echo "Deleting dir: {}" \; -exec rm -rf {} \;
echo "/data/disk0 cleaned"