from azure.batch import BatchServiceClient
import azure.batch.models as batchmodels
from azure.batch.batch_auth import SharedKeyCredentials

def canKillBatchPool(batchServiceClient: BatchServiceClient, job_id: str):
    tasks = batchServiceClient.task.list(job_id)
    # incomplete_tasks = [task for task in tasks if
    #                         task.state != batchmodels.TaskState.completed]
    # print(f"....{len(incomplete_tasks)}...")
    incomplete_tasks = []
    count = 0
    for task in tasks:
        count = count + 1
        if task.state != batchmodels.TaskState.completed:
            incomplete_tasks.append(task)
    if count == 0:
        print("task count is 0")
        return False
    if not incomplete_tasks:
        print()
        return True
    
    return False

if __name__ == '__main__':
    POOL_ID = 'BatchCopyPool' #You can put any name you like
    JOB_ID = 'BatchCopyJob' #You can put any name you like

    credentials = SharedKeyCredentials("<batch_account_name>",
        "<batch_account_key>")
    batch_client = BatchServiceClient(
        credentials,
        batch_url="https://bigdatasnapshotsync.westus3.batch.azure.com")

    can = canKillBatchPool(batch_client, JOB_ID)
    print(can)
    if can:
        batch_client.job.delete(JOB_ID)
        batch_client.pool.delete(POOL_ID)