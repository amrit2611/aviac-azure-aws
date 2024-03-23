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
docker_image_name = "amrit2611/aviac-odm-1"

# ...............creating JOB_ID and POOL_ID.................
timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")
JOB_ID = f"aviactest-{timestamp}"
POOL_ID = JOB_ID


# ...............handling command-line arguments.............
# if len(sys.argv) != 4:
#     print("Usage: python main.py BUCKET KEY OUTPUT")
#     sys.exit(1)
# BUCKET = sys.argv[1]
# KEY = sys.argv[2]
# OUTPUT = sys.argv[3]
# print("Argument 1:", BUCKET)
# print("Argument 2:", KEY)
# print("Argument 3:", OUTPUT)


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
    container_conf = batchmodels.ContainerConfiguration(type='dockerCompatible', container_image_names=[docker_image_name])
    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="microsoft-dsvm",
                offer="ubuntu-hpc",
                sku="2004",
                version="latest"
            ),
            container_configuration=container_conf,
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


def add_tasks(batch_service_client: BatchServiceClient, job_id: str, dockerfile_path: str, script_path: str):
    print(f'Adding a single task to job [{job_id}]...')
    
    dockerfile_name = os.path.basename(dockerfile_path)
    script_name = os.path.basename(script_path)
    dockerfile_resource = batchmodels.ResourceFile(file_path=dockerfile_name, http_url=dockerfile_path)
    script_resource = batchmodels.ResourceFile(file_path=script_name, http_url=script_path)
    
    bucket_name = "test-drone-yard-droneyard-dronephotosbucket1549df6-u35knpllluqx"
    key_name = "batch-test-1"
    output_name = "output"
    
    task_container_settings = batchmodels.TaskContainerSettings(
        image_name=docker_image_name,
        container_run_options=' --workdir /')
    
    # command = f'/bin/bash cmd /c set BUCKET={bucket_name}  -c \'entry.sh BUCKET KEY OUTPUT\''
    command = f'/bin/bash -c "./entry.sh "test-drone-yard-droneyard-dronephotosbucket1549df6-u35knpllluqx" "batch-test-1" "output""'
    task = batchmodels.TaskAddParameter(
               id=f'Task-{job_id}',
               command_line=command,
            # command_line='/bin/sh -c \"echo \'hello world\' > $AZ_BATCH_TASK_WORKING_DIR/output.txt\"',    
               resource_files=[
                   dockerfile_resource, script_resource
               ],
               container_settings=task_container_settings)
    batch_service_client.task.add(job_id, task)


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
    
    dockerfile_url = "https://aviacbatchstorage.blob.core.windows.net/sourcecontainer/Dockerfile"
    script_url = "https://aviacbatchstorage.blob.core.windows.net/sourcecontainer/entry.sh"
    
    credentials = SharedKeyCredentials(config.BATCH_ACCOUNT_NAME,
        config.BATCH_ACCOUNT_KEY)
    batch_client = BatchServiceClient(      
        credentials,
        batch_url=config.BATCH_ACCOUNT_URL)
    
    try:
        create_pool(batch_client, POOL_ID)
        create_job(batch_client, JOB_ID, POOL_ID)
        add_tasks(batch_client, JOB_ID, dockerfile_url, script_url)
        wait_for_tasks_to_complete(batch_client, JOB_ID, datetime.timedelta(minutes=15))
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
print("************************     End     ***********************")