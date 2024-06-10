#!/bin/bash
#This shell script could be considered as part of the cluster bootstrap scripts
set -x 

sudo azcopy copy https://<Blob_container_path>/disk_space_check.sh /home/hadoop/
sudo azcopy copy https://<Blob_container_path>/tmp_file_remover.sh /home/hadoop/

sudo chmod +x /home/hadoop/disk_space_check.sh /home/hadoop/tmp_file_remover.sh

echo "copy done!!!"

if ! grep -q "disk_space_check.sh" /etc/crontab;then
#set the checking window as every 5 min, and it won't be added to crontab again if the command already exists
echo "*/5 * * * * root /home/hadoop/disk_space_check.sh >> /var/log/disk_space_check.log" | sudo tee -a /etc/crontab
echo "new cron job has been appended successfully!!!"
else echo "cron job already exists!!!"
fi
