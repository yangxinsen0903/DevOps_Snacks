## Notes:
upload the shell scripts `disk_space_check.sh` and `tmp_file_remover.sh` in advance to a Blob container path under a specific Azure stroage account

set up crontab to check disk usage of system disk every 5 minutes.

`disk_space_check.sh` runs every 5 minutes to check if the disk usage of system disk is higher than 85%. If so, it will trigger `tmp_file_remover.sh` to delete files in pattern like "az_blob_filesystem_*" or "*block_*" or "*part-*" older than 3 days.

To manually remove files, you can also execute 
`sudo find $path -name $pattern -type f -mtime +3 -print0 | xargs -0 -I {} sh -c 'echo "Removing file: {}"; rm "{}"'`