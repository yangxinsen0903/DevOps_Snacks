# SREUS-4026

## Notes:
Scpripts for removing HDFS/non-HDFS files older than 3 days for EMR cluster prod-ailab-sort-minute-l-001.

### For HDFS files
`clean_sparkStaging.sh` has been deployed under `/home/hadoop` at master node `10.207.84.62` to remove `.sparkStaging` files older than 3 days.  
Cronjob set to run the script hourly. While the default Cron environment does not support command `hdfs`, need to add environment variables at the beginning of the script or in `/etc/crontab`.  
Timestamp and the deleted directories will be printed.

### For non-HDFS files
`add_cronjob_ailab_sort_minute.sh` should be executed on SDP platform on core nodes, which will download script `disk0_clean.sh` to local directory `/home/hadoop` form Azure stroage account `https://usp1prod1abcdw.blob.core.windows.net/config/dw/scripts` and set up crontab to clean disk `/data/disk0` every hour.

`disk0_clean.sh` runs hourly to remove cache files older than 3 days under directory `/data/disk0/hadoop/yarn/local/usercache/hadoop/filecache`.  
Timestamp and directories deleted will be printed.

To manually remove files, execute `sudo find "$path" -mindepth 1 -maxdepth 1 -type d -mtime +3 -exec echo "Deleting dir: {}" \; -exec rm -rf {} \;` after setting up path and pattern.