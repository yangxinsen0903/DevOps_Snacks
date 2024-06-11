#!/bin/bash

set -x 

sudo azcopy copy https://xxx/disk0_clean.sh /home/hadoop/

sudo chmod +x /home/hadoop/disk0_clean.sh

echo "copy done!!!"

if ! grep -q "disk0.sh" /etc/crontab;then
echo "0 */1 * * * root /home/hadoop/disk0_clean.sh >> /var/log/disk0_clean.log" | sudo tee -a /etc/crontab
echo "new cron job has been appended successfully!!!"
else echo "cron job already exists!!!"
fi
