## Notes:
1. Create a VM (CentOS) under the region where you deploy the resources, install python3(3.9) pip and lib packages
pip install azure-identity
pip install azure-cli-core
pip install azure-storage-blob
pip install azure-batch
pip install azure-mgmt-batch

2. Create an Azure batch account
3. Create a user assigned managed identity，and associate it with Azure VM and the batch account
    a. add storage account Storage blob data owner/contributor role
    b. owner role for batach account itself
    c. identity clientid/tenantid/secret
4. Storage account SAS token
5. Upload the python scripts to VM, and create a two cronjobs, one for generating daily snapshots using batch account, another one for deleting the job and pool when it is done
6. Ensure Azure VM and batch account can access to the corresponding storage account

./configure prefix=/usr/local/python3

yum install zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gcc make libffi-devel

make && make install

ln -s /usr/local/python3/bin/python3.9 /usr/bin/python3