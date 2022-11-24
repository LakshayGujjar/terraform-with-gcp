# Copyright 2022 Google LLC. This software is provided as is,
# without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.
"""
DAG for Security scorecard - Full scorecard API
"""
from airflow import models, DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow import AirflowException

from dependencies.logging_utils import setup_logger
from dependencies.secret_manager_util import get_secret
from dependencies.yaml_reader_utils import config_reader

from google.oauth2 import service_account
from googleapiclient import _auth
from google.cloud import storage

import json
import requests
import logging
from datetime import datetime, timezone, timedelta, date
import time

logger = setup_logger(logging.getLogger(__name__))

logger.info('Security Scorecard API pipeline execution started')

logger.debug("Reading variables from YAML configuration file")
category = "SECURITY_SCORECARD"
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)

try:
    # variable extraction
    PROJECT_ID = common_config_data['GCP']['PROJECT_ID']
    REGION = common_config_data['GCP']['REGION']
    SECRET_ID = config_data['SECRET_ID']
    SCHEDULE_INTERVAL = config_data['DAILY_SCHEDULE_INTERVAL']
    BUCKET_NAME = config_data['OUTPUT_BUCKET']
    SECRET_VERSION_ID = config_data['SECRET_VERSION']
    DATAFLOW_TEMPLATE = config_data['DATAFLOW_TEMPLATE_BUCKET_PATH']
    DAG_OWNER = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    DAG_RETRIES = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_RETRIES']
    DAG_START_DATE = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']


except (ValueError, KeyError) as error:
    logger.error("failed to read config variable, %s", error)
    raise KeyError(f"failed to read config variable, {error}")

default_args = {
    'owner': DAG_OWNER,
    'start_date':DAG_START_DATE,
    'retries': DAG_RETRIES,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}

'''Functions START'''
def GetPortfolios(ti):
    """
    #1 Get all portfolios you have access to
        args: API token
        return: portfolio ids
    """
    token= ti.xcom_pull(key='token_xcom', task_ids='getDataFromScorecardAPI')
    try:
        logger.info("get the all portfolios ")
        url = "https://api.securityscorecard.io/portfolios"

        headers = {
            "accept": "application/json; charset=utf-8",
            "Authorization": "Token {} ".format(token)
        }
        response = requests.get(url, headers=headers)
        temp = json.loads(response.text)
        entries = temp['entries']
        port_ids = [sub['id'] for sub in entries]
        port_ids = port_ids[1:2] # need to be change
        ti.xcom_push(key='port_ids_xcom', value=port_ids)
        logger.debug('No. of Portfolios: {}'.format(len(port_ids)))
        logger.info(f"FULL scorecard DAG generated no. of Portfolios: {len(port_ids)} successfully")

    except Exception as err:
        logger.error("Security Scorecard API pipeline failed due to the error: {}".format(err))
        logger.critical("Security Scorecard API pipeline failed due to the error: {}".format(err))
        raise AirflowException("Security Scorecard API pipeline failed due to the error: {}".format(err))

    #return port_ids


def GetCompanies(ti):
    """
    #2 Get all companies in a portfolio
        args: portfolio ids, API token
        return: company ids under each portfolio
    """
    try:
        logger.info("Get all companies in a portfolio")
        token= ti.xcom_pull(key='token_xcom', task_ids='getDataFromScorecardAPI')
        port_ids= ti.xcom_pull(key='port_ids_xcom', task_ids='GetPortfolios')
        company_ids = []
        for port_id in port_ids:
            url = "https://api.securityscorecard.io/portfolios/{}/companies".format(port_id)

            headers = {
                "accept": "application/json; charset=utf-8",
                "Authorization": "Token {} ".format(token)
            }

            response1 = requests.get(url, headers=headers)
            temp = json.loads(response1.text)
            entries = temp['entries']
            company_id_sl = [sub['domain'] for sub in entries]
            company_ids.append(company_id_sl)
            ti.xcom_push(key='company_ids_xcom', value=company_ids)
            logger.info("scorecard generated all the companies")


    except Exception as err:
        logger.error("Security Scorecard API pipeline failed due to the error: {}".format(err))
        logger.critical("Security Scorecard API pipeline failed due to the error: {}".format(err))
        raise AirflowException("Security Scorecard API pipeline failed due to the error: {}".format(err))


    #return company_ids


def Generate_FullScorecard(ti):
    """
    #3 Generate a Full Scorecard report
        args: domain ids, API token
        return: company ids
    """
    try:
        '''flatten nested list'''
        logger.info("Generate a Full Scorecard report")
        token= ti.xcom_pull(key='token_xcom', task_ids='getDataFromScorecardAPI')
        company_ids= ti.xcom_pull(key='company_ids_xcom', task_ids='GetCompanies')
        c_ids = [item for sublist in company_ids for item in sublist]
        de_dup_cids = list(set(c_ids)) # remove duplicate domain ids
        

        logger.debug('No. of Total Companies: {}'.format(len(de_dup_cids)))
        i = 1

        for company_id in de_dup_cids:
            url = "https://api.securityscorecard.io/reports/full-scorecard-json"
            payload = {"params": {"scorecard_identifier": "{}".format(company_id)}}
            headers = {
                "accept": "application/json; charset=utf-8",
                "content-type": "application/json",
                "Authorization": "Token {}".format(token)
            }
            response = requests.post(url, json=payload, headers=headers)
            time.sleep(1)
            logger.debug(i)
            i += 1
        ti.xcom_push(key=' de_dup_cids_xcom', value=de_dup_cids)
        logger.debug("FULL scorecard DAG generated reports successfully")
        logger.info("FULL scorecard DAG generated reports successfully")

    except Exception as err:
        logger.error("Security Scorecard API pipeline failed due to the error: {}".format(err))
        logger.critical("Security Scorecard API pipeline failed due to the error: {}".format(err))
        raise AirflowException("Security Scorecard API pipeline failed due to the error: {}".format(err))
        

    


def Access_RecentReports(ti):
    """
    #4 Get recently generated reports
        args: domain ids, API token
        return: report download https urls
    """
    try:
        logger.info("Fetching recently generated report")
        token= ti.xcom_pull(key='token_xcom', task_ids='getDataFromScorecardAPI')
        url_list = []
        de_dup_cids= ti.xcom_pull(key='de_dup_cids_xcom', task_ids='Generate_FullScorecard')
        for company_id in de_dup_cids:
            url3 = "https://api.securityscorecard.io/reports/recent"

            headers = {
                "accept": "application/json; charset=utf-8",
                "Authorization": "Token {}".format(token)
            }
            response = requests.get(url3, headers=headers)
            temp = json.loads(response.text)
            time.sleep(1)
            '''Fetching recently generated report through filters'''
            dicts = temp['entries']
            search = {"domain": "{}".format(company_id)}
            b = list(filter(lambda b: b.get("params") == search, dicts))  # searching with company id
            if b:
                logger.debug(company_id)
                try:
                    durl = [sub['download_url'] for sub in b]
                    url_list.append(durl[0])
                except Exception as e:
                    logger.error(f"Download URL N/A{e}")
                    continue

        logger.debug('No. of URLS: {}'.format(len(url_list)))
        ti.xcom_push(key='url_list_xcom', value=url_list)
        logger.info("Get recently Generated reports")
        

    except Exception as err:
        logger.error("Security Scorecard API pipeline failed due to the error: {}".format(err))
        logger.critical("Security Scorecard API pipeline failed due to the error: {}".format(err))
        raise AirflowException("Security Scorecard API pipeline failed due to the error: {}".format(err))

   


def current_timestamp_UTC():
    """
    #4.1 Get current timestamp
        return: current timestamp UTC
    """
    try:
        logger.info("fetch current timestamp")
        now = datetime.now(timezone.utc)
        logger.info('Current UTC Time:{}'.format(now))
        return now

    except Exception as e:
        logger.error("failed to get current timestamp error is:{}".format(e))
        logger.critical("failed to get current timestamp error is:{}".format(e))
        raise AirflowException("failed to get current timestamp error is:{}".format(e))


def DownloadReport_to_GCS(ti):
    """
    #5 Download a generated report and write to GCS
        args: domain ids, download urls and API token
        return: None
    """
    try:
        logger.info("starting-Download a generated report")
        token= ti.xcom_pull(key='token_xcom', task_ids='getDataFromScorecardAPI')
        url_list= ti.xcom_pull(key='url_list__xcom', task_ids='Access_RecentReports')
        for durl in url_list:
            url = "{}".format(durl)
            headers = {"Authorization": "Token {}".format(token)}
            response = requests.get(url, headers=headers)
            scorecard_data = response.text
            dir_name = current_timestamp_UTC()
            year = dir_name.year
            month = dir_name.month
            day = dir_name.day
            second=dir_name.second
            destination_blob_name = "scorecard/{0}/{1}/{2}/report_{3}.json".format(year, month, day,second)
            #gcs_bucket_prefix = "gs://{0}/scorecard/{1}/{2}/{3}/".format(BUCKET_NAME,year, month, day)
            # Pushing GCS bucket path for Dataflow
            #ti.xcom_push(key='gcs_bucket_name', value=gcs_bucket_prefix)
            gcs_client = storage.Client()
            bucket = gcs_client.get_bucket(BUCKET_NAME)
            blob = bucket.blob(destination_blob_name)
            resp_json = []
            '''convert ndjson to json'''
            for ndjson_line in scorecard_data.splitlines():
                if not ndjson_line.strip():
                    continue  # ignore empty lines
                json_line = json.loads(ndjson_line)
                resp_json.append(json_line)

            blob.upload_from_string(json.dumps(resp_json))
        logger.debug(f"FULL scorecard DAG loading data successfully completed{destination_blob_name}")
        logger.info(f"FULL scorecard DAG loading data successfully completed{destination_blob_name}")

    except Exception as err:
        logger.error("Security Scorecard API pipeline failed due to the error: {}".format(err))
        logger.critical("Security Scorecard API pipeline failed due to the error: {}".format(err))
        raise AirflowException("Security Scorecard API pipeline failed due to the error: {}".format(err))


def getDataFromScorecardAPI(PROJECT_ID, SECRET_ID, SECRET_VERSION_ID, ti):
    """
    #Full scorecard API - main function
        args: None
        return: API token
    """
    try:
        logger.info('Starting - Full scorecard API')
        secret_api_key = get_secret(PROJECT_ID, SECRET_ID, SECRET_VERSION_ID)
        token = secret_api_key
        logger.info(token)
        ti.xcom_push(key='token_xcom', value=token)
        logger.debug("FULL scorecard DAG execution successfully completed")

    except Exception as err:
        logger.error("Security Scorecard API pipeline failed due to the error: {}".format(err))
        logger.critical("Security Scorecard API pipeline failed due to the error: {}".format(err))
        raise AirflowException("Security Scorecard API pipeline failed due to the error: {}".format(err))

  
'''Functions END'''

with DAG(
        dag_id="securityscorecard_fullscorecard",
        schedule_interval=SCHEDULE_INTERVAL,
        default_args=default_args,
        tags=["Full Scorecard API"],
        description="Dag for the security scorecard API",
) as dag:

    opr_start = DummyOperator(task_id='START')

    opr_api_call = PythonOperator(
        task_id="getDataFromScorecardAPI",
        python_callable=getDataFromScorecardAPI,
        op_kwargs={'PROJECT_ID':PROJECT_ID, 'SECRET_ID':SECRET_ID, 'SECRET_VERSION_ID':SECRET_VERSION_ID}
    )

    opr_get_all_portfolios=PythonOperator(
        task_id="GetPortfolios",
        python_callable=GetPortfolios
        
    )
    opr_get_all_companies=PythonOperator(
        task_id="GetCompanies",
        python_callable=GetCompanies
    )
    opr_generate_scorecard_report =PythonOperator(
        task_id="Generate_FullScorecard",
        python_callable=Generate_FullScorecard
        
    )
    opr_get_recently_generated_reports=PythonOperator(
        task_id="Access_RecentReports",
        python_callable=Access_RecentReports
        
    )
    opr_download_scorecard_report=PythonOperator(
        task_id="DownloadReport_to_GCS",
        python_callable=DownloadReport_to_GCS
        
    )
    
    '''opr_dataflow_call = DataflowTemplatedJobStartOperator(
        task_id="GCS_to_BQ",
        parameters={
            "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='API_to_GCS', key='gcs_bucket_name') }}"
        },
        template=DATAFLOW_TEMPLATE,
        location=REGION,
        job_name="scorecard"
    )'''

    opr_end = DummyOperator(task_id='END')
     
    opr_start >> opr_api_call >> opr_get_all_portfolios >> opr_get_all_companies >> opr_generate_scorecard_report >> opr_get_recently_generated_reports >> opr_download_scorecard_report >> opr_end 
  

    logger.info(f"Full scorecard execution completed successfully on {date.today()}")
