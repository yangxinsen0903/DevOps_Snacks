###########################################################
# pip install azure-identity
# pip install azure-cli-core
# pip install azure-storage-blob
# pip install azure-batch
# pip install azure-mgmt-batch
###########################################################

from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient, BlobClient
from azure.batch import BatchServiceClient
# from azure.batch.batch_auth import SharedKeyCredentials
import azure.mgmt.batch as mgmtBatch
import azure.mgmt.batch.models as mgmtModels
import azure.batch.models as batchmodels
from azure.common.credentials import ServicePrincipalCredentials
import datetime, time, uuid, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

##################################init variables###################################
subscriptionId = "<your_subscription_id>"
userAssignedIdentity = "/subscriptions/xxx/resourcegroups/xxx/providers/Microsoft.ManagedIdentity/userAssignedIdentities/xxx"
userAssignedIdentityClientId = '<your_manage_identity_client_id>'

storageAccountName = "<storage_account_name>"
fromContainerName = "<source_container_name>"
toContainerName = "<target_container_name>"
sourceFileContainerName = "sourcefile"
sfContainerSAS = "<storage_account_sas_token>"
prefix = "<source_path_copy_from>"

client_id = "<service_principal_client_id>"
client_secret = "<service_principal_secret>"
tenant_id = "<service_principal_tenant_id>"
resource = "https://batch.core.windows.net/"

batchAccountRG = "<batch_account_resource_group>"
batchAccountName = "<batch_account_name>"
batchAccountKey = "<batch_account_key>"
batchAccountRegion = "westus3" #This is an example
vmSize='Standard_D2ds_v4' #This is an example
targetDedicatedNodes=50 #Depends on the data volume
subnet_id = "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Network/virtualNetworks/xxx/subnets/xxx"


AZCOPY_CONCURRENCY_VALUE = 1000
###################################################################################

accountUrl = f"https://{storageAccountName}.blob.core.windows.net"
credential = DefaultAzureCredential()

# batchAccountCredential = SharedKeyCredentials(batchAccountName, batchAccountKey)
# batch_client = BatchServiceClient(
#     batchAccountCredential,
#     batch_url=f"https://{batchAccountName}.{batchAccountRegion}.batch.azure.com")

spCredential = ServicePrincipalCredentials(
    client_id = client_id,
    secret = client_secret,
    tenant = tenant_id,
    resource = resource
)

batch_client = BatchServiceClient(
    spCredential,
    batch_url=f"https://{batchAccountName}.{batchAccountRegion}.batch.azure.com"
)

def splitSourceBlobNames():
    containerFromClient = ContainerClient(account_url=accountUrl,container_name=fromContainerName,credential=credential)
    # blobList = containerFromClient.list_blob_names()
    blobList = containerFromClient.list_blob_names(name_starts_with =  prefix)

    uploadPool = ThreadPoolExecutor(max_workers = 10)
    futures = []
    blobInput = []
    batchInputUrls = []
    index = 0
    for page in blobList.by_page():
        for blobName in page:
            # filter out the files if blobName.startswith(".hive") or "dt=" in blobName:
            if ".hive" in blobName or "dt=" in blobName:
                continue
            blobInput.append(blobName.split(prefix)[1])
            index = index + 1
            if index % 5000 == 0:
                blobData = "\n".join(blobInput)
                futures.append(uploadPool.submit(uploadToBlob, blobData))
                blobInput = []
        #     if index == 500:
        #         break
        # if index == 500:
        #     break
    if len(blobInput) != 0:
        blobData = "\n".join(blobInput)
        futures.append(uploadPool.submit(uploadToBlob, blobData))
    blobInput = []

    for future in as_completed(futures):  
        batchInputUrls.append(future.result()) 

    print(index)
    return batchInputUrls

def uploadToBlob(blobData):
    now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S.%f")
    blobName=f"blob-{str(now)}-{uuid.uuid4()}.txt"
    blobClient = BlobClient(account_url=accountUrl,container_name=sourceFileContainerName,blob_name=blobName,credential=credential)
    blobClient.upload_blob(blobData)
    # return generateBlobUrl("batchinput", blobName)
    return batchmodels.ResourceFile(
        http_url=f"{accountUrl}/{sourceFileContainerName}/{blobName}?{sfContainerSAS}",
        file_path=blobName
    )

def create_pool(batch_service_client: BatchServiceClient, pool_id: str):
    print(f'Creating pool [{pool_id}]...')
    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="canonical",
                offer="0001-com-ubuntu-server-focal",
                sku="20_04-lts",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 20.04"),
        vm_size=vmSize,
        target_dedicated_nodes=targetDedicatedNodes,
        start_task=batchmodels.StartTask(
            command_line="/bin/bash -c \"wget https://azcopyvnext.azureedge.net/releases/release-10.21.2-20231106/azcopy_linux_amd64_10.21.2.tar.gz -P /tmp && cd /tmp && tar zxvf azcopy_linux_amd64_10.21.2.tar.gz && chmod -R 777 azcopy_linux_amd64_10.21.2 && pwd && ls -l azcopy_linux_amd64_10.21.2\"",
            wait_for_success=True,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=batchmodels.ElevationLevel.admin))
        ),
        network_configuration=batchmodels.NetworkConfiguration(
            subnet_id=subnet_id,
            enable_accelerated_networking=True
        ),
        target_node_communication_mode="simplified"
    )

    batch_service_client.pool.add(new_pool)

def update_pool(poolName: str):
    batchClient = mgmtBatch.BatchManagementClient(credential=DefaultAzureCredential(), subscription_id=subscriptionId)
    userIdentity = mgmtModels.UserAssignedIdentities()
    batchPoolIdentity = mgmtModels.BatchPoolIdentity(type=mgmtModels.PoolIdentityType.USER_ASSIGNED,user_assigned_identities={userAssignedIdentity: userIdentity})
    poolParam = mgmtModels.Pool(identity=batchPoolIdentity)
    batchClient.pool.update(resource_group_name=batchAccountRG,account_name=batchAccountName, pool_name=poolName,parameters=poolParam)

def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    print(f'Creating job [{job_id}]...')

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)

def add_tasks(batch_service_client: BatchServiceClient, job_id: str, resource_input_files: list):
    print(f'Adding {resource_input_files} tasks to job [{job_id}]...')

    tasks = []

    today = datetime.datetime.now().strftime("%Y%m%d")

    for idx, input_file in enumerate(resource_input_files):
        command = f"/bin/bash -c \"export AZCOPY_CONCURRENCY_VALUE={AZCOPY_CONCURRENCY_VALUE} && /tmp/azcopy_linux_amd64_10.21.2/azcopy login --identity --identity-client-id '{userAssignedIdentityClientId}' && /tmp/azcopy_linux_amd64_10.21.2/azcopy cp 'https://{storageAccountName}.blob.core.windows.net/{fromContainerName}/{prefix}*' 'https://{storageAccountName}.blob.core.windows.net/{toContainerName}/backup/{today}/dw.db/' --list-of-files={input_file.file_path} --overwrite=false\""
        tasks.append(batchmodels.TaskAddParameter(
            id=f'Task{idx}',
            command_line=command,
            resource_files=[input_file],
            constraints=batchmodels.TaskConstraints(max_task_retry_count=3)
        )
        )

    batch_service_client.task.add_collection(job_id, tasks)

def wait_for_tasks_to_complete(batch_service_client: BatchServiceClient, job_id: str,
                               timeout: datetime.timedelta):
    timeout_expiration = datetime.datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
    
        time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))

def print_batch_exception(batch_exception: batchmodels.BatchErrorException):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print(f'{mesg.key}:\t{mesg.value}')
    print('-------------------------------------------')

#main
if __name__ == '__main__':
    start_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample start: {start_time}')
    print()

    inputFiles = splitSourceBlobNames()

    mid_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample mid: {mid_time}')
    elapsed_time = mid_time - start_time
    print(f'Mid Elapsed time: {elapsed_time}')
    print()

    POOL_ID = 'BatchCopyPool' #You can put any name you like
    JOB_ID = 'BatchCopyJob' #You can put any name you like

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, POOL_ID)

        # update pool
        update_pool(POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, JOB_ID, POOL_ID)

        # Add the tasks to the job.
        add_tasks(batch_client, JOB_ID, inputFiles)

        print("Success! All tasks commited.")
        
        end_time = datetime.datetime.now().replace(microsecond=0)
        print(f'Sample end: {end_time}')
        elapsed_time = end_time - start_time
        print(f'Elapsed time: {elapsed_time}')
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise
