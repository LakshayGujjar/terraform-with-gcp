"""
DAG for account and assets -
Description: dag will extract data from s3 bucket to gcp bucket

1. Extract data from s3 to gcp

Dataflow Operator Flow:

1. trigger dataflow pipelines for transformation

"""
# Importing dependancies
from google.cloud import storage
import os
import time
import logging
import json
from airflow import DAG
from airflow.operators import python_operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow import AirflowException

# from dependencies.logging_utils import setup_logger
from dependencies.logging_utils import setup_logger
from dependencies.yaml_reader_utils import config_reader
from dependencies.secret_manager_util import get_secret
from airflow.models import Variable

from datetime import datetime, timezone, timedelta

logger = setup_logger(logging.getLogger(__name__))
logger.info('Account and Assets pipeline execution started')

try:
    import boto3
except ImportError as err:
    logger.error('boto3 is required to run data_replicator_sample_consumer.  Please "pip install boto3"!')
    raise AirflowException('boto3 is required to run data_replicator_sample_consumer.  Please "pip install boto3"!')

# read configuration parameters from custom config operator
category = "CROWDSTRIKE_ACCOUNT_AND_ASSETS"
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)

try:
    PROJECT_ID = common_config_data['GCP']['PROJECT_ID']
    REGION = common_config_data['GCP']['REGION']
    SECRET_ID = config_data['SECRETMANAGER_SECRET_ID']
    SCHEDULE_INTERVAL = config_data['HALF_AN_HOURLY_SCHEDULE_INTERVAL']
    BUCKET_NAME = config_data['OUTPUT_BUCKET']
    SECRET_VERSION_ID = config_data['SECRET_VERSION']
    NONMANAGED_DATAFLOW_TEMPLATE_LOCATION = config_data['NONMANAGED_DATAFLOW_TEMPLATE_LOCATION']
    MANAGED_DATAFLOW_TEMPLATE_LOCATION = config_data['MANAGED_DATAFLOW_TEMPLATE_LOCATION']
    USERINFO_DATAFLOW_TEMPLATE_LOCATION = config_data['USERINFO_DATAFLOW_TEMPLATE_LOCATION']
    APPINFO_DATAFLOW_TEMPLATE_LOCATION = config_data['APPINFO_DATAFLOW_TEMPLATE_LOCATION']
    AIDMASTER_DATAFLOW_TEMPLATE_LOCATION = config_data['AIDMASTER_DATAFLOW_TEMPLATE_LOCATION']
    DAG_OWNER = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    DAG_DEPENDS_ON_PAST = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_DEPENDS_ON_PAST']
    DAG_START_DATE = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']
    DAG_RETRIES = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_RETRIES']
    SQS_VISIBILITY_TIMEOUT = config_data['SQL_VISIBILITY_TIMEOUT']
    SQS_REGION_NAME = config_data['SQS_REGION_NAME']


except (ValueError, KeyError) as error:
    logger.error("failed to read config variable, %s", error)
    raise KeyError(f"failed to read config variable, {error}")

DEFAULT_DAG_ARGS = {
    "owner": DAG_OWNER,
    "start_date": DAG_START_DATE,
    "retries": DAG_RETRIES,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
}


def download_message_files(msg, AWS_KEY, AWS_SECRET, OUTPUT_PATH):
    """
        Downloads the files from s3 referenced in msg and places them in OUTPUT_PATH.
        download_message_files function will iterate through every file listed at msg['filePaths'],
        move it to a local path with name "{OUTPUT_PATH}/{s3_path}",
        and then call handle_file(path).
    """
    try:
        # Construct output path for this messages files.
        msg_output_path = os.path.join(OUTPUT_PATH, msg['pathPrefix'])
        # Ensure directory exists at output path
        if not os.path.exists(msg_output_path):
            os.makedirs(msg_output_path)
        for s3_file in msg['files']:
            # extract s3_bucket data path into s3_path variable
            s3_path = s3_file['path']
            local_path = os.path.join(OUTPUT_PATH, s3_path)
            dataFoldercheck = s3_path.split("/", 1)[0]
            logger.debug("datacheckFolderName:{}".format(dataFoldercheck))
            s3_path = "s3://{}/{}".format(msg['bucket'], s3_path)
            logger.debug("s3 bucket path:{}".format(s3_path))
            # check data coming in data folder or fdv2 if its data folder skip uploadation part
            if dataFoldercheck != "data":
                os.system('AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s gsutil -m cp -R %s %s/' % (
                    AWS_KEY, AWS_SECRET, s3_path, local_path))
                logger.debug('AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s gsutil -m cp -R %s %s/' % (
                    AWS_KEY, AWS_SECRET, s3_path, local_path))
    except Exception as e:
        logger.debug("Failed to transfer s3 files to gcs bucket error is:{}".format(e))
        raise AirflowException("Failed to transfer s3 files to gcs bucket error is:{}".format(e))


def consume_data_replicator(AWS_KEY, AWS_SECRET, QUEUE_URL, OUTPUT_PATH):
    """
        Consume from data replicator and track number of messages/files/bytes downloaded.
    """
    try:
        sleep_time, msg_cnt, file_cnt, byte_cnt = 1, 0, 0, 0

        # Connect to our CrowdStrike provided SQS queue
        sqs = boto3.resource('sqs', region_name=SQS_REGION_NAME,
                             aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
        s3 = boto3.client('s3', region_name=SQS_REGION_NAME,
                          aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)
        queue = sqs.Queue(url=QUEUE_URL)
        logger.debug("inside consume data replicator")
        while True:
            """ We want to continuously poll the queue for new messages.
                Receive messages from queue if any exist (NOTE: receive_messages() only receives a few messages at a
                time, it does NOT exhaust the queue)"""
            for msg in queue.receive_messages(VisibilityTimeout=SQS_VISIBILITY_TIMEOUT):
                msg_cnt += 1
                body = json.loads(msg.body)  # grab the actual message body
                download_message_files(body, AWS_KEY, AWS_SECRET, OUTPUT_PATH)
                file_cnt += body['fileCount']
                byte_cnt += body['totalSize']
                # msg.delete() must be called or the message will be returned to the SQS queue after
                # VISIBILITY_TIMEOUT seconds
                msg.delete()
                time.sleep(sleep_time)
            logger.debug("transfering file from s3 bucket to gcs completed for SQS URL:{}".format(QUEUE_URL))
            logger.debug("Messages consumed: %i\tFile count: %i\tByte count: %i" %
                         (msg_cnt, file_cnt, byte_cnt))
            break
    except Exception as error:
        logger.error("failed to consume data from SQS URL error is:{}".format(error))
        raise AirflowException("failed to consume data from SQS URL error is:{}".format(error))


def getdestinationFolderPath():
    """
        fetching secret value from secret manager
    """
    # Build the resource name of the secret version.
    try:
        # fetch current time stamp
        currentTimeStamp = datetime.now(timezone.utc)
        # currentTimeStamp.year : fetch year from currenttimestamp , currentTimeStamp.strftime("%B") : fetch month from currentTimeStamp ,currentTimeStamp.strftime("%H:%M:%S") : fetch hour from currentTimeStamp
        bucketfolderPath = "crowdstrikeFdr/{}/{}/{}/{}".format(currentTimeStamp.year, currentTimeStamp.strftime("%B"),
                                                               currentTimeStamp.day,
                                                               currentTimeStamp.strftime("%H:%M:%S"))
        destinationPath = "gs://{}/{}/fdrv2".format(BUCKET_NAME, bucketfolderPath)
        return destinationPath
    except Exception as error:
        logger.error("failed to fetch data value from api error is:{}".format(error))
        raise AirflowException("failed to fetch data value from api error is:{}".format(error))


with DAG(
        dag_id="crowdstrike_account_and_assets_data_ingestion_v1",
        tags=["account-and-asset-DAG"],
        schedule_interval=SCHEDULE_INTERVAL,
        default_args=DEFAULT_DAG_ARGS,
        catchup=False,
) as dag:
    opr_start = DummyOperator(task_id='START')
    compledted_uploadation=DummyOperator(task_id='gcs_data_uploadation_compledted')

    get_destination_gcs_folder_path = python_operator.PythonOperator(
        task_id="get_destination_gcs_folder_path",
        python_callable=getdestinationFolderPath)

    dataflow_run_for_aidmaster = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_aidmaster",
        parameters={
            "gcsBucket": "{{ task_instance.xcom_pull(task_ids='get_destination_gcs_folder_path', key='return_value') }}/aidmaster"
        },
        template=AIDMASTER_DATAFLOW_TEMPLATE_LOCATION,
        location=REGION,
        job_name="aidmaster"
    )

    dataflow_run_for_appinfo = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_appinfo",
        parameters={
            "gcsBucket": "{{ task_instance.xcom_pull(task_ids='get_destination_gcs_folder_path', key='return_value') }}/appinfo"
        },
        template=APPINFO_DATAFLOW_TEMPLATE_LOCATION,
        location=REGION,
        job_name="appinfo"
    )

    dataflow_run_for_userinfo = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_userinfo",
        parameters={
            "gcsBucket": "{{ task_instance.xcom_pull(task_ids='get_destination_gcs_folder_path', key='return_value') }}/userinfo"
        },
        template=USERINFO_DATAFLOW_TEMPLATE_LOCATION,
        location=REGION,
        job_name="userinfo"
    )

    dataflow_run_for_managed = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_managed",
        parameters={
            "gcsBucket": "{{ task_instance.xcom_pull(task_ids='get_destination_gcs_folder_path', key='return_value') }}/managedassets"
        },
        template=MANAGED_DATAFLOW_TEMPLATE_LOCATION,
        location=REGION,
        job_name="managed"
    )

    dataflow_run_for_nonmanaged = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_nonmanaged",
        parameters={
            "gcsBucket": "{{ task_instance.xcom_pull(task_ids='get_destination_gcs_folder_path', key='return_value') }}/notmanaged"
        },
        template=NONMANAGED_DATAFLOW_TEMPLATE_LOCATION,
        location=REGION,
        job_name="nonmanaged"
    )

    secret_api_key = get_secret(PROJECT_ID, SECRET_ID, SECRET_VERSION_ID)
    secret_api_key = json.loads(secret_api_key)
    for key, value in secret_api_key.items():
        # fetching username,passward,url from secret manager for key
        AWS_KEY = value['username']
        AWS_SECRET = value['password']
        QUEUE_URL = value['url']
        dynamicTask = PythonOperator(
            task_id='{0}'.format(key),
            dag=dag,
            provide_context=True,
            python_callable=consume_data_replicator,

            op_kwargs={
                'OUTPUT_PATH': "{{ task_instance.xcom_pull(task_ids='get_destination_gcs_folder_path', key='return_value') }}",
                'AWS_KEY': AWS_KEY.strip(), 'AWS_SECRET': AWS_SECRET.strip(), 'QUEUE_URL': QUEUE_URL.strip()})

        get_destination_gcs_folder_path.set_downstream(dynamicTask)
        dynamicTask.set_downstream(compledted_uploadation)
        compledted_uploadation.set_downstream(dataflow_run_for_managed)
        compledted_uploadation.set_downstream(dataflow_run_for_nonmanaged)
        compledted_uploadation.set_downstream(dataflow_run_for_appinfo)
        compledted_uploadation.set_downstream(dataflow_run_for_aidmaster)
        compledted_uploadation.set_downstream(dataflow_run_for_userinfo)

    opr_end = DummyOperator(task_id='END')

    opr_start >> get_destination_gcs_folder_path
    [dataflow_run_for_aidmaster, dataflow_run_for_appinfo, dataflow_run_for_userinfo, dataflow_run_for_managed, dataflow_run_for_nonmanaged]>> opr_end
    # parallel composr through