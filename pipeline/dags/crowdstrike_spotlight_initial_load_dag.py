#  Copyright 2022 Google LLC. This software is provided as is,
#  without warranty or representation for any use or purpose.
#  Your use of it is subject to your agreement with Google.

"""DAG to ingest Crowdstrike Spotlight API responses to GCS bucket and BigQuery"""

import json
import requests
from datetime import datetime, timezone, timedelta
import logging

from google.oauth2 import service_account
from googleapiclient import _auth
from google.cloud import storage, bigquery

from airflow import models, DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow import AirflowException
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from dependencies.logging_utils import setup_logger
from dependencies.secret_manager_util import get_secret
from dependencies.yaml_reader_utils import config_reader



logger = setup_logger(logging.getLogger(__name__))
logger.info('Crowdstrike Spotlight pipeline execution started')

category = "CROWDSTRIKE_SPOTLIGHT"
logger.debug("Reading variables from YAML configuration file")
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)

try:
    # variable extraction
    PROJECT_ID = common_config_data['GCP']['PROJECT_ID']
    REGION = common_config_data['GCP']['REGION']
    SECRET_ID = config_data['SECRET_ID']
    SECRET_VERSION_ID = config_data['SECRET_VERSION']
    CROWDSTRIKE_URL = config_data['CROWDSTRIKE_URL']
    DATAFLOW_COMBINED_TEMPLATE = config_data['DATAFLOW_COMBINED_TEMPLATE']
    SCHEDULE_INTERVAL = config_data['SCHEDULE_INTERVAL']
    BUCKET_NAME = config_data['OUTPUT_BUCKET']
    DAG_OWNER = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    DAG_RETRIES = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_RETRIES']
    DAG_START_DATE = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']


except (ValueError, KeyError) as error:
    logger.error("failed to read environment variable, %s", error)
    raise KeyError(f"failed to read environment variable, {error}")

default_args = {
    'owner': DAG_OWNER,
    'start_date':DAG_START_DATE,
    'retries': DAG_RETRIES,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}

'''Functions START'''
def generate_token(username, password, ti):
    """
    #1 Generate an OAuth2 access token for agency credentials
        :return: API token
    """
    try:
        logger.info('Generating access token for agency credentials')
        crowdstrike_url = CROWDSTRIKE_URL
        payload = "client_id={0}&client_secret={1}".format(username, password)

        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded"
        }
        response = requests.post(crowdstrike_url, data=payload, headers=headers)
        temp = json.loads(response.text)
        token = temp['access_token']
        ti.xcom_push(key='token', value=token)
        logger.info('Access token for agency credentials generated successfully')

    except Exception as err:
        logger.error("failed to generate token for Agency credentials due to error: {}".format(err))
        logger.critical("Crowdstrike Spotlight API pipeline failed in generating token due to error: {}".format(err))
        raise AirflowException(f"failed in extracting config variable due to error: {err}")


def current_timestamp_UTC():
    """
    Get current timestamp
    """
    try:
        now = datetime.now(timezone.utc)
        # print('Current UTC Time:{}'.format(now))
        logger.info("successfully got the current timestamp.")

    except Exception as e:
        logger.error("failed to get current timestamp error is:{}".format(e))

    return now


def Combined_Vulnerabilties( agency, INITIAL_PIPELINE_RUN_DATETIME, ti):
    """
    #2 Combined Endpoint API
        :return: API response in JSON
    """
    try:

        logger.debug(INITIAL_PIPELINE_RUN_DATETIME)

        urlc = "https://api.crowdstrike.com/spotlight/combined/vulnerabilities/v1/"
        token = ti.xcom_pull(key='token', task_ids='processing_tasks_{}.Generate_Token'.format(agency))

        logger.info(f"token:{token}")

        #updated_timestamp less than current date time
        params = {
            'filter': 'updated_timestamp:<\'{}\''.format(INITIAL_PIPELINE_RUN_DATETIME),
        }
        headers = {
            "Authorization": "bearer {}".format(token),
            'Content-Type': 'application/json',
        }

        logger.debug(params)

        responsec = requests.get(urlc, params=params, headers=headers)
        response_data = json.loads(responsec.text)
        ti.xcom_push(key='response_data', value=response_data)
        logger.debug("Combined Endpoint Initial  PULL status code: {}".format(responsec.status_code))



    except Exception as err:
        logger.error("failed to Combined Endpoint Initial Load due to error: {}".format(err))
        logger.critical("Crowdstrike Spotlight API pipeline failed in Combined Endpoint Initial Load due to error: {}".format(err))
        raise AirflowException(f"failed in Combined Endpoint Initial Load due to error: {err}")



def Write_to_GCS(agency, BUCKET_NAME,  ti):
    """
    #3 Load JSON (response_data) response to GCS
    """
    try:
        dir_name = current_timestamp_UTC()
        year = dir_name.year
        month = dir_name.month
        day = dir_name.day
        hour = dir_name.hour
        response_data = ti.xcom_pull(key='response_data',task_ids='processing_tasks_{}.Combined_Vulnerabilties'.format(agency))
        logger.info(response_data )
        destination_blob_name = "spotlight_initial/{0}/{1}/{2}/{3}/{4}.json".format(year, month, day, hour, agency)
        gcs_bucket_prefix = "gs://{0}/spotlight_initial/{1}/{2}/{3}/{4}".format(BUCKET_NAME, year, month, day, hour)
        ti.xcom_push(key='gcs_bucket_name', value=gcs_bucket_prefix)
        gcs_client = storage.Client()
        bucket = gcs_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(response_data))
        logger.debug('Loaded spotlight API initial data')

    except Exception as err:
        logger.error("failed in writing files to GCS due to error: {}".format(err))
        logger.critical("Crowdstrike Spotlight API pipeline failed in writing files to GCS due to error: {}".format(err))
        raise AirflowException(f"failed in writing files to GCS due to error: {err}")



'''Program starts here'''
with DAG(
        dag_id="Spotlight_Initial_Combined",
        schedule_interval= None,
        start_date=datetime(2022, 11, 11),
        default_args=default_args,
        tags=["Initial Load"],
) as dag:
    # start
    opr_start = DummyOperator(task_id='START')
    # connect
    opr_fork = DummyOperator(task_id='FORK')
    # end
    opr_end = DummyOperator(task_id='END')

    secret_api_key = get_secret(PROJECT_ID, SECRET_ID, SECRET_VERSION_ID)
    secret_api_key = eval(secret_api_key)

    # Fetch username and password of 43 agencies through iteration
    groups = []
    for key in secret_api_key.items():
        agency = key[0]
        logger.debug(agency)
        username = secret_api_key[agency]['username']
        password = secret_api_key[agency]['password']

        # task group
        with TaskGroup(group_id=f'processing_tasks_{agency}') as processing_tasks:
            opr_generate_token = PythonOperator(
                task_id='Generate_Token',
                python_callable=generate_token,
                op_kwargs={'username': username, 'password': password}
             )
            opr_combined_vulnerabilties=PythonOperator(
                task_id='combined_vulnerabilties',
                python_callable=Combined_Vulnerabilties,
                op_kwargs={'agency': agency,
                         "INITIAL_PIPELINE_RUN_DATETIME": "{{ execution_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}"}

            )

            opr_combined_to_gcs = PythonOperator(
                task_id='Combined_to_GCS',
                python_callable=Write_to_GCS,
                op_kwargs={'agency': agency, 'BUCKET_NAME': BUCKET_NAME}
             )
            opr_generate_token >> opr_combined_vulnerabilties >> opr_combined_to_gcs
            groups.append(processing_tasks)


    opr_combined_dataflow = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_combined_endpoint",
        parameters={"gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='processing_tasks_NJ-RATE.API_to_GCS', key='gcs_bucket_name') }}"},
        template=DATAFLOW_COMBINED_TEMPLATE,
        location=REGION,
        job_name="spotlight_combined"
    )

    opr_start >> groups >> opr_fork >> opr_combined_dataflow >> opr_end