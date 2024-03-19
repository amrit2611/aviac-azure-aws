import datetime
import io
import os
import sys
import time
from datetime import datetime as dt
import config
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
from azure.batch.models import ResourceFile
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas
    )

DEFAULT_ENCODING = "utf-8"

# ...............creating JOB_ID and POOL_ID.................
timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
JOB_ID = f"aviactest-{timestamp}"
POOL_ID = JOB_ID


# ...............handling command-line arguments.............
if len(sys.argv) != 4:
    print("Usage: python main.py BUCKET KEY OUTPUT")
    sys.exit(1)
BUCKET = sys.argv[1]
KEY = sys.argv[2]
OUTPUT = sys.argv[3]
print("Argument 1:", BUCKET)
print("Argument 2:", KEY)
print("Argument 3:", OUTPUT)


def query_yes_no(question: str, default: str = "yes") -> str:
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError(f"Invalid default answer: '{default}'")
    choice = default
    while 1:
        user_input = input(question + prompt).lower()
        if not user_input:
            break
        try:
            choice = valid[user_input[0]]
            break
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")
    return choice


def print_batch_exception(batch_exception: batchmodels.BatchErrorException):
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
        vm_size=config.POOL_VM_SIZE,
        target_dedicated_nodes=config.POOL_NODE_COUNT,
        target_low_priority_nodes=config.POOL_SPOT_COUNT
    )
    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    print(f'Creating job [{job_id}]...')
    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))
    batch_service_client.job.add(job)


def add_tasks(batch_service_client: BatchServiceClient, job_id: str, dockerfile_path: str, script_path: str, bucket_url: str, key_url: str, output_url: str):
    print(f'Adding a single task to job [{job_id}]...')
    command = f'/bin/bash -c "cp {dockerfile_path} . && cp {script_path} . && docker build -t my_docker_image . && docker run --rm my_docker_image /bin/bash -c \'{os.path.basename(script_path)}\' `bucket_url` `key_url` `output_url`"'
    task = batchmodels.TaskAddParameter(
               id=f'Task-{job_id}',
               command_line=command,
               resource_files=[
                   ResourceFile(file_path=os.path.basename(dockerfile_path), blob_source=dockerfile_path),
                   ResourceFile(file_path=os.path.basename(script_path), blob_source=script_path)
               ])
    batch_service_client.task.add_collection(job_id, task)


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


if __name__ == '__main__':
    start_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample start: {start_time}')
    print()
    
    def generate_blob_sas_uri(container_name, blob_name):
        blob_service_client = BlobServiceClient(account_url= f"https://{config.STORAGE_ACCOUNT_NAME}.{config.STORAGE_ACCOUNT_DOMAIN}/", credential=config.STORAGE_ACCOUNT_KEY)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        sas_token = generate_blob_sas(account_name=config.STORAGE_ACCOUNT_NAME, container_name=container_name, blob_name=blob_name, account_key=config.STORAGE_ACCOUNT_KEY, permission=BlobSasPermissions(read=True), expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=5))
        return blob_client.url + '?' + sas_token
    
    dockerfile_sas_url = generate_blob_sas_uri(config.STORAGE_ACCOUNT_CONTAINER, 'Dockerfile')
    script_sas_url = generate_blob_sas_uri(config.STORAGE_ACCOUNT_CONTAINER, 'entry.sh')
    
    credentials = SharedKeyCredentials(config.BATCH_ACCOUNT_NAME,
        config.BATCH_ACCOUNT_KEY)
    batch_client = BatchServiceClient(      
        credentials,
        batch_url=config.BATCH_ACCOUNT_URL)
    # dockerfile_path = "Dockerfile"
    # script_path = "entry.sh"
    try:
        create_pool(batch_client, POOL_ID)
        create_job(batch_client, JOB_ID, POOL_ID)
        add_tasks(batch_client, JOB_ID, dockerfile_sas_url, script_sas_url, BUCKET, KEY, OUTPUT)
        wait_For_tasks_to_complete(batch_client, JOB_ID, datetime.timedelta(minutes=30))
        print("Success! All tasks reached the 'Completed' state within the ""specified timeout period.")
        end_time = datetime.datetime.now().replace(microsecond=0)
        print()
        print(f'Sample end: {end_time}')
        elapsed_time = end_time - start_time
        print(f'Elapsed time: {elapsed_time}')
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise
    finally:
        if query_yes_no('Delete pool?') == 'yes':
            batch_client.pool.delete(POOL_ID)
print("End")