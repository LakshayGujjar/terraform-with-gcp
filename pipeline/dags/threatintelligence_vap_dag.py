"""
DAG for threatintel vap api's - vap
Description: dag will extract data from vap api will upload it to gcs bucket and trigger dataflow pipelines
Python Operator Flow:

1. Extract data from vap api
2. upload vap api response to gcs bucket

Dataflow Operator Flow:

1. trigger dataflow pipelines for transformation

"""
#Importing dependancies
from google.cloud import storage
import time

from airflow import DAG
from airflow.operators import python_operator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import AirflowException

from dependencies.logging_utils import setup_logger
from dependencies.secret_manager_util import get_secret
from dependencies.yaml_reader_utils import config_reader

import json,logging,requests
from datetime import datetime, timezone,timedelta

logger = setup_logger(logging.getLogger(__name__))
logger.info('ThreatIntel VAP pipeline execution started')

# read configuration parameters from custom config operator
category = "THREAT_INTELLIGENCE_VAP"
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)


try:
    PROJECT_ID = common_config_data['GCP']['PROJECT_ID']
    REGION = common_config_data['GCP']['REGION']
    SECRET_ID = config_data['SECRETMANAGER_SECRET_ID']
    SCHEDULE_INTERVAL = config_data['HOURLY_SCHEDULE_INTERVAL']
    BUCKET_NAME = config_data['VAP_OUTPUT_BUCKET']
    SECRET_VERSION_ID = config_data['SECRET_VERSION']
    PAGE_SIZE = config_data['SIZE']
    DATAFLOW_TEMPLATE = config_data['DATAFLOW_TEMPLATE_BUCKET_PATH']
    DAG_OWNER= common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    DAG_DEPENDS_ON_PAST= common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_DEPENDS_ON_PAST']
    DAG_START_DATE= common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']
    DAG_RETRIES= common_config_data['AIRFLOW'] ['DEFAULT_DAG_ARGS_RETRIES']
    VAP_API_URL= config_data['VAP_API_URL']
except (ValueError, KeyError) as e:
    logger.error("Failed to read config variable, %s", e)
    raise KeyError(f"Failed to read config variable, {e}")


try:
     # read credential from secret manager
    logger.info("Fetching data from secret manager.")
    secretApiKey = json.loads(get_secret(PROJECT_ID, SECRET_ID, SECRET_VERSION_ID))
    principal = secretApiKey['principal']
    secret = secretApiKey['secret']
    logger.info("Successfully to fetched data from secret manager.")
except Exception as error:
    logger.error("Failed to fetch data from secret manager error is:{}".format(error))
    raise AirflowException("Failed to fetch data from secret manager error is:{}".format(error))

DEFAULT_DAG_ARGS = {
    "owner": DAG_OWNER,
    "start_date": DAG_START_DATE,
    "retries": DAG_RETRIES,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}


def uploadDataToBucket(vapApiData,bucketFolderPath):
    """
    api response data will upload to gcs bucket
    --input parameters:
        vapApiData : contain response of vap api
        bucketFolderPath : destination folder path
    """
    try:
        logger.info("create destination gcs bucket folder path")
        # create destination gcs bucket folder path
        destinationPath = "threatIntelVAP/{}/vap.json".format(bucketFolderPath)
        logger.debug("data uploadation start to bucket path is:gs://{}/{}".format(BUCKET_NAME, destinationPath))
        # create gcs bucket client
        storageClient = storage.Client()
        vapBucket = storageClient.get_bucket(BUCKET_NAME)
        vapApiBlob = vapBucket.blob(destinationPath)
        #upload vap api response to gcs bucket
        vapApiBlob.upload_from_string(json.dumps(vapApiData))
        logger.info("threatIntel VAP DAG task  : Vap api response uploaded successfully to bucket path is : gs://{}/{}".format(BUCKET_NAME, destinationPath))
        logger.debug("Vap api response uploaded successfully to bucket")
    except Exception as error:
        logger.error("Failed to upload vap api response to bucket Error is:{}".format(error))
        logger.critical(
            "Failed to upload vap api response to bucket Error is:{}".format(error))
        raise AirflowException(f"Failed to upload vap api response to bucket Error is: {error}")



def fetchDataFromVapAPI():
    """function will fetch data from vap api """
    try:
        logger.info("threatIntel VAP DAG task satrted : extracting data from vap api")
        #calling custom function to fetch credentials from secret manager
        currentTimeStamp = datetime.now(timezone.utc)
        # currentTimeStamp.year : fetch year from currenttimestamp , currentTimeStamp.strftime("%B") : fetch month from currentTimeStamp ,currentTimeStamp.strftime("%H:%M:%S") : fetch hour from currentTimeStamp
        bucketfolderPath = "{}/{}/{}/{}".format(currentTimeStamp.year, currentTimeStamp.strftime("%B"), currentTimeStamp.day, currentTimeStamp.strftime("%H:%M:%S"))
        destinationBucketPath="gs://{}/threatIntelVAP/{}/vap.json".format(BUCKET_NAME,bucketfolderPath)
        # fetching data from vap api
        logger.debug("Extracting data from vap api")
        page = 1
        #The maximum number of VAPs to produce in the response. The attackIndex value determine the order of results. Defaults to 1000.
        size = PAGE_SIZE
        while True:
            logger.info("Updating the vapApiURL with page and size")
            #update vapApiURL endpoint with page and size 
            vapApiURL = VAP_API_URL.format(page, size)
            #Getting the response from vapApiURL by passing the auth values
            responseAPI = requests.get(vapApiURL, auth=(principal, secret))
            logger.debug(responseAPI.status_code)
            if responseAPI.status_code == 200:
                # converting vap api response to dict format
                vapApiResponse = responseAPI.json()
                if len(vapApiResponse["users"]) != 0:
                    logger.debug("total number of users:{}".format(len(vapApiResponse["users"])))
                    if page == 1:
                        vapData = vapApiResponse
                    else:
                        # when we send too many requets to api then api will raise 429 error code so added 5 seconds sleep
                        time.sleep(10)
                        vapData["users"].extend(vapApiResponse["users"])
                    logger.debug("after extent number of users: {}".format(len(vapData["users"])))
                    page = page + 1
                else:
                    break
            elif responseAPI.status_code == 429:
                logger.debug("The number of queries to this endpoint are limited by a simple, rolling 24-hour throttle. Once exceeded, the API will start returning 429 HTTP status codes until 24 hours past the oldest request has elapsed.")
                break
            else:
                break
        logger.debug("Successfully data fetched from vap api")
        uploadDataToBucket(vapData,bucketfolderPath)
        return destinationBucketPath
    except requests.exceptions.HTTPError as error:
        if responseAPI.status_code == 400:
            logger.error("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
            logger.critical("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
        elif responseAPI.status_code == 401:
            logger.error("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
            logger.critical("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
        elif responseAPI.status_code == 500:
            logger.error("The service has encountered an unexpected situation and is unable to give a better response to the request")
            logger.critical("The service has encountered an unexpected situation and is unable to give a better response to the request")
        else:
            logger.error("Failed to fetch data from vap api error is: {}".format(error))
            logger.critical("Failed to fetch data from vap api error is: {}".format(error))
        raise AirflowException("Failed to fetch data from vap api error is : {}" .format(error))
    except Exception as error:
        logger.error("Failed to fetch data from vap api error is:{}".format(error))
        logger.critical("Failed to fetch data from vap api error is:{}".format(error))
        raise AirflowException("Failed to fetch data from vap api error is : {}" .format(error))


with DAG(
    dag_id="threatIntel_vap",
    tags=["threatIntel-vap-dag"],
    schedule_interval= SCHEDULE_INTERVAL,
    default_args=DEFAULT_DAG_ARGS,
    catchup=False,
) as dag:
    #start dummy operator
    opr_start = DummyOperator(task_id='START')

    fetchData = python_operator.PythonOperator(
        task_id='extract_data_from_vap_api',
        python_callable=fetchDataFromVapAPI)

    dataflowDag = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_vap_api",
        # custom input parameter for dataflow gcsBucketPath : vap api file path
        parameters={
            "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='extract_data_from_vap_api', key='return_value') }}"
        },
        location=REGION,
        job_name="vap",
        template= DATAFLOW_TEMPLATE,
    )
    

    #end dummy operator
    opr_end = DummyOperator(task_id='END')

    opr_start >> fetchData >> dataflowDag >> opr_end