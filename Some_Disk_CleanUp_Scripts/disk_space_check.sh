#!/bin/bash

# set the disk usage threshold as 85%
threshold=85
current_usage=$(df -h --output=pcent / | awk 'NR==2{print $1}' | tr -d '%')

if [ $current_usage -gt $threshold ]; then
    # Execute your desired actions or commands here
    /home/hadoop/tmp_file_remover.sh
fi

echo "Done $(date +%Y-%m-%d\ %H:%M:%S)"