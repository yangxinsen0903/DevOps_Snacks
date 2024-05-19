#!/bin/bash

path="/tmp/"  # Specify the path to the directory
pattern=("az_blob_filesystem_*" "*block_*" "*part-*")  # some file pattern examples

# Find files matching the specified patterns, and delete the data older than 3 days 
for pattern in "${pattern[@]}"; do
    find "$path" -name "$pattern" -type f -mtime +3 -print0 |
    while IFS= read -r -d '' file; do
        echo "Removing file: $file"
        rm "$file"
    done
done