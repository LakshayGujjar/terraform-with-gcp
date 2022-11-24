#  Copyright 2022 Google LLC. This software is provided as is,
#  without warranty or representation for any use or purpose.
#  Your use of it is subject to your agreement with Google.

"""DAG to ingest Crowdstrike Spotlight Subsequent load of Combined and Remediations API responses to GCS bucket and BigQuery"""

import json
import requests
from datetime import datetime, timezone, timedelta
import logging

from google.oauth2 import service_account
from googleapiclient import _auth
from google.cloud import secretmanager, storage, bigquery

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

import pandas as pd
import numpy as np

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
    DATAFLOW_COMBINED_TEMPLATE = config_data['DATAFLOW_COMBINED_TEMPLATE']
    DATAFLOW_REMEDIATIONS_TEMPLATE = config_data['DATAFLOW_REMEDIATIONS_TEMPLATE']
    BUCKET_NAME = config_data['OUTPUT_BUCKET']
    CROWDSTRIKE_URL = config_data['CROWDSTRIKE_URL']
    COMBINED_VULNERABILITIES_URL = config_data['COMBINED_VULNERABILITIES_URL']
    REMEDIATIONS_URL = config_data['REMEDIATIONS_URL']
    DAG_OWNER = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    DAG_RETRIES = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_RETRIES']
    DAG_START_DATE = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']

except (ValueError, KeyError) as error:
    logger.error("failed to read environment variable, %s", error)
    raise KeyError(f"failed to read environment variable, {error}")

default_args = {
    'owner': DAG_OWNER,
    'start_date': DAG_START_DATE,
    'retries': DAG_RETRIES,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'provide_context': True,
}

logger.info('Crowdstrike Spotlight DAG owner name is NJCCIC')


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
        logger.debug('Current UTC Time:{}'.format(now))

    except Exception as e:
        logger.error("failed to get current timestamp error is:{}".format(e))
        raise AirflowException("failed to get current timestamp error is:{}".format(e))

    return now


def combined_endpoint_subsequent(agency, LAST_PIPELINE_RUN_DATETIME, CURRENT_PIPELINE_RUN_DATETIME, ti):
    """
    Combined endpoint subsequent pipeline
        return: API data
    """
    try:
        logger.debug(LAST_PIPELINE_RUN_DATETIME)
        logger.debug(CURRENT_PIPELINE_RUN_DATETIME)

        combined_vulnerabilities_url = COMBINED_VULNERABILITIES_URL

        token = ti.xcom_pull(key='token',task_ids='processing_tasks_{}.Generate_Token'.format(agency))

        logger.info(f"token:{token}")


        params = {
            'filter': 'updated_timestamp:>\'{0}\'''+''updated_timestamp:<\'{1}\''.format(LAST_PIPELINE_RUN_DATETIME,
                                                                                         CURRENT_PIPELINE_RUN_DATETIME)
        }

        headers = {
            "Authorization": "bearer {}".format(token),
            'Content-Type': 'application/json',
        }

        logger.debug(params)

        responsec = requests.get(combined_vulnerabilities_url, params=params, headers=headers)
        combined_data = json.loads(responsec.text)
        ti.xcom_push(key='combined_data', value=combined_data)
        logger.debug("Combined Endpoint subsequent PULL status code: {}".format(responsec.status_code))
        logger.info('Combined Endpoint subsequent pipeline')

    except Exception as err:
        logger.error("failed to Combined Endpoint subsequent Load due to error: {}".format(err))
        logger.critical(
            "Crowdstrike Spotlight API pipeline failed in Combined Endpoint subsequent Load due to error: {}".format(err))
        raise AirflowException(f"failed in Combined Endpoint subsequent load due to error: {err}")


def extract_remediations_id(agency, ti):
    """
    Extract remediations IDs from Combined API
        return: list of remediations IDs
    """
    try:
        logger.info('Extracting remediation IDs from Combined API')
        combined_data = ti.xcom_pull(key='combined_data',task_ids='processing_tasks_{}.Combined_Endpoint'.format(agency))

        logger.info(f"combined data: {combined_data}")
        data_list = [combined_data]
        df = pd.DataFrame(data_list)
        if 'meta' in df.columns:
            df = df.drop(['meta'], axis=1)  # delete metadata cols

        df1 = pd.DataFrame()
        remediations_ids = []

        # resources
        if 'resources' in df.columns:
            if df['resources'][0] != []:
                # resources
                if 'resources' in df.columns:
                    df = pd.concat([df.explode('resources').drop(['resources'], axis=1),
                                    df.explode('resources')['resources'].apply(pd.Series)], axis=1)

                # apps
                if 'apps' in df.columns:
                    df = pd.concat([df.explode('apps').drop(['apps'], axis=1),
                                    df.explode('apps')['apps'].apply(pd.Series)], axis=1)

                    # apps: product_name_version,sub_status,remediation,evaluation_logic
                    df.rename(
                        columns=({'product_name_version': 'apps_product_name_version', 'sub_status': 'apps_sub_status',
                                  'remediation': 'apps_remediation', 'evaluation_logic': 'apps_evaluation_logic'}),
                        inplace=True)

                    # apps_remediation
                    df1 = df["apps_remediation"].apply(pd.Series)

                    # apps_remediation: ids
                    df1.rename(columns=({'ids': 'apps_remediation_ids'}), inplace=True)

                    if 'apps_remediation_ids' in df1.columns:
                        df1['apps_remediation_ids'] = df1['apps_remediation_ids'].apply(pd.Series)

                        df1 = df1.applymap(str)
                        df1 = df1[df1.apps_remediation_ids != 'nan']
                        df1.rename(columns=({'apps_remediation_ids': 'ids'}), inplace=True)
                        df1 = df1.drop_duplicates()
                        remediations_ids = df1['ids'].tolist()
                        ti.xcom_push(key='remediations_ids', value=remediations_ids)

        else:
            logger.debug("Got empty list in resources key")

    except Exception as err:
        logger.error("failed to extract remediations ids due to error: {}".format(err))
        logger.critical(
            "Crowdstrike Spotlight API pipeline failed in extracting remediations ids due to error: {}".format(err))
        raise AirflowException(f"failed in extracting remediations ids due to error: {err}")


def Remediations(agency, ti):
    """Remediations endpoint
        return: remediations data in json"""
    try:
        logger.info('Extracting remediation ids from remediation data')
        remediations_ids = ti.xcom_pull(key='remediations_ids',
                                        task_ids='processing_tasks_{}.Extract_Remediations_IDs'.format(agency))
        if not remediations_ids:
            logger.debug("remidiation id's length is zero")
        elif len(remediations_ids) < 400:

            logger.info("Number of remediations_ids = {}".format(len(remediations_ids)))
            urlr = REMEDIATIONS_URL
            token = ti.xcom_pull(key='token', task_ids='processing_tasks_{}.Generate_Token'.format(agency))

            headers = {
                "Authorization": "bearer {}".format(token),
                'Content-Type': 'application/json',
            }

            remediations_ids_param = 'ids=' + '&ids='.join(remediations_ids)
            params = '{}'.format(remediations_ids_param)
            responser = requests.get(urlr, params=params, headers=headers)
            remediations_data = json.loads(responser.text)
            ti.xcom_push(key='remediations_data', value=remediations_data)
            remediations_status = responser.status_code
            logger.debug("Remediations Endpoint subsequent PULL status code:{}".format(remediations_status))

        else:
            logger.info("Remediations Ids have length: {}".format(len(remediations_ids)))

    except Exception as err:
        logger.error("failed to remediations Endpoint subsequent Load due to error: {}".format(err))
        logger.critical(
            "Crowdstrike Spotlight API pipeline failed in remediations Endpoint due to error: {}".format(err))
        raise AirflowException(f"failed in remediations Endpoint subsequent load due to error: {err}")


def Write_to_GCS(agency, endpoint, BUCKET_NAME, ti):
    """
    #3 Load JSON (response_data) response to GCS
    """
    try:
        dir_name = current_timestamp_UTC()
        year = dir_name.year
        month = dir_name.month
        day = dir_name.day
        hour = dir_name.hour

        if endpoint == "combined":
            response_data = ti.xcom_pull(key='combined_data', task_ids='processing_tasks_{}.Combined_Endpoint'.format(agency))

            destination_blob_name = "spotlight_{0}/{1}/{2}/{3}/{4}/{5}.json".format(endpoint, year, month, day, hour,
                                                                                    agency)
            gcs_bucket_prefix = "gs://{0}/spotlight_{1}/{2}/{3}/{4}/{5}".format(BUCKET_NAME, endpoint, year, month, day,
                                                                                hour)
            ti.xcom_push(key='gcs_bucket_combined', value=gcs_bucket_prefix)

        elif endpoint == "remediations":
            response_data = ti.xcom_pull(key='remediations_data', task_ids='processing_tasks_{}.Remediations_Endpoint'.format(agency))

            destination_blob_name = "spotlight_{0}/{1}/{2}/{3}/{4}/{5}.json".format(endpoint, year, month, day, hour,
                                                                                    agency)
            gcs_bucket_prefix = "gs://{0}/spotlight_{1}/{2}/{3}/{4}/{5}".format(BUCKET_NAME, endpoint, year, month, day,
                                                                                hour)
            ti.xcom_push(key='gcs_bucket_remediations', value=gcs_bucket_prefix)

        else:
            logger.debug('API endpoint is not combined or remediations')

        gcs_client = storage.Client()
        bucket = gcs_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(response_data))
        logger.debug('Loaded spotlight API data')

    except Exception as err:
        logger.error("failed in writing files to GCS due to error: {}".format(err))
        logger.critical(
            "Crowdstrike Spotlight API pipeline failed in writing files to GCS due to error: {}".format(err))
        raise AirflowException(f"failed in writing files to GCS due to error: {err}")


'''Program starts here'''
with DAG(
        dag_id="Spotlight_Combined_Remediations",
        schedule_interval="0 * * * *",
        start_date=datetime(2022, 11, 18),
        default_args=default_args,
        tags=["Subsequent Load"],
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

            opr_combined_endpoint = PythonOperator(
                task_id='Combined_Endpoint',
                python_callable=combined_endpoint_subsequent,
                op_kwargs={'agency': agency,
                    "LAST_PIPELINE_RUN_DATETIME": "{{ prev_data_interval_start_success.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
                    "CURRENT_PIPELINE_RUN_DATETIME": "{{ execution_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}"}
            )

            opr_combined_to_gcs = PythonOperator(
                task_id='Combined_to_GCS',
                python_callable=Write_to_GCS,
                op_kwargs={'agency': agency, "endpoint": "combined", 'BUCKET_NAME': BUCKET_NAME}
            )

            opr_extract_remediations_ids = PythonOperator(
                task_id='Extract_Remediations_IDs',
                python_callable=extract_remediations_id,
                op_kwargs={'agency': agency}
            )

            opr_remediations_endpoint = PythonOperator(
                task_id='Remediations_Endpoint',
                python_callable=Remediations,
                op_kwargs={'agency': agency}

            )

            opr_remediations_to_gcs = PythonOperator(
                task_id='Remediations_to_GCS',
                python_callable=Write_to_GCS,
                op_kwargs={'agency': agency, "endpoint": "remediations", 'BUCKET_NAME': BUCKET_NAME}
            )

            opr_generate_token >> opr_combined_endpoint >> opr_combined_to_gcs >> opr_extract_remediations_ids >> opr_remediations_endpoint >> opr_remediations_to_gcs
            groups.append(processing_tasks)

    # dataflow
    opr_combined_dataflow = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_combined_endpoint",
        parameters={
            "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='processing_tasks_NJ-RATE.Combined_to_GCS', key='gcs_bucket_combined') }}"},
        template=DATAFLOW_COMBINED_TEMPLATE,
        location=REGION,
        job_name="spotlight_combined"
    )

    opr_remediations_dataflow = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_remediations_endpoint",
        parameters={
            "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='processing_tasks_NJ-RATE.Remediations_to_GCS', key='gcs_bucket_remediations') }}"},
        template=DATAFLOW_REMEDIATIONS_TEMPLATE,
        location=REGION,
        job_name="spotlight_remediations"
    )

    opr_start >> groups >> opr_fork >> [opr_combined_dataflow, opr_remediations_dataflow] >> opr_end
