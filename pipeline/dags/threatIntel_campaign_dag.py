"""
DAG for threatintel api's - campaign api
Description: dag will extract data from campaign api will upload it to gcs bucket and trigger dataflow pipelines
Python Operator Flow:

1. Extract campaign ID from campaign ID  api
2. Extract data for those campaignID's from campaign api
3. upload data to gcs bucket

Dataflow Operator Flow:

1. trigger dataflow pipelines for transformation

"""
#Importing dependancies
from google.cloud import storage

from airflow import DAG
from airflow.operators import python_operator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import AirflowException
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG

#from dependencies.logging_utils import setup_logger
from dependencies.logging_utils import setup_logger
from dependencies.yaml_reader_utils import config_reader
from dependencies.secret_manager_util import get_secret
from airflow.decorators import task

from datetime import datetime, timezone, timedelta

import json,logging,requests
from requests.auth import HTTPDigestAuth

logger = setup_logger(logging.getLogger(__name__))
logger.info('ThreatIntel campaign pipeline execution started')

# read configuration parameters from custom config operator
category = "THREAT_INTELLIGENCE_CAMPAIGN"
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)


try:
    PROJECT_ID = common_config_data['GCP']['PROJECT_ID']
    REGION = common_config_data['GCP']['REGION']
    SECRET_ID = config_data['SECRETMANAGER_SECRET_ID']
    SCHEDULE_INTERVAL = config_data['DAILY_SCHEDULE_INTERVAL']
    BUCKET_NAME = config_data['THREAT_INTELLIGENCE_API_OUTPUT_BUCKET']
    SECRET_VERSION_ID = config_data['SECRET_VERSION']
    PAGE_SIZE = int(config_data['SIZE'])
    DAG_OWNER = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    DAG_DEPENDS_ON_PAST = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_DEPENDS_ON_PAST']
    DAG_START_DATE = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']
    DAG_RETRIES = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_RETRIES']
    CAMPAIGN_ID_API_URL = config_data['CAMPAIGN_ID_API_URL']
    CAMPAIGN_API_URL = config_data['CAMPAIGN_API_URL']
    CAMPAIGN_DATAFLOW_TEMPLATE_LOACATION = config_data["DATAFLOW_TEMPLATE_BUCKET_PATH_CAMPAIGN"]

except (ValueError, KeyError) as error:
    logger.error("failed to read config variable, %s", error)
    raise KeyError(f"failed to read config variable, {error}")

try:
    # read credential from secret manager
    logger.info("Fetching data from secret manager.")
    secretApiKey = get_secret(PROJECT_ID, SECRET_ID, SECRET_VERSION_ID)
    secretApiKey=json.loads(secretApiKey)
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
    'provide_context':True,
}



def uploadDataToBucket(campaignApiResponse, campaignFileName, bucketFolderPath):
    """
        api response data will upload to gcs bucket
        --input parameters:
            campaignApiResponse : contain response of campaign api
            campaignFileName : contain campaign file name for e.g campaign_{campaignID}
            bucketFolderPath : destination folder path
    """
    try:
        logging.info("create destination gcs bucket folder path")
        # create destination gcs bucket folder path
        prefix = bucketFolderPath.split("/", 3)[3]
        destinationPath = "{}{}.json".format(prefix, campaignFileName)
        logger.debug("camapign api response uploading start at bucket path is:{}".format( bucketFolderPath))
        #create gcs bucket client
        storage_client = storage.Client()
        # Retrieve a bucket using bucket name
        threatIntelBucket = storage_client.get_bucket(BUCKET_NAME)
        campaignApiResponseBlob = threatIntelBucket.blob(destinationPath)
        #upload api response to bucket
        campaignApiResponseBlob.upload_from_string(json.dumps(campaignApiResponse))
        logging.info("Uploaded data to bukcet on location:{}".format(destinationPath))
    except Exception as error:
        logger.error("Failed to upload campaign api response to bucket Error is:{}".format(error))
        logging.critical(
            "Failed to upload campaign api response to bucket Error is:{}".format(error))
        raise AirflowException(f"Failed to upload campaign api response to bucket Error is: {error}")

def getIdFromCampaignAPI(dag_start_time,dag_end_time,ti):
    """
        function will fetch campaign id's from campaignID api and then extract data for those campaign id's from campaign api
    """
    try:
        logging.info("Fetching campaign id's from campaignID api")
        # create destination gcs bucket folder path according to current timestamp
        currentTimeStamp = datetime.now(timezone.utc)
        # currentTimeStamp.year : fetch year from currenttimestamp , currentTimeStamp.strftime("%B") : fetch month from currentTimeStamp ,currentTimeStamp.strftime("%H:%M:%S") : fetch hour from currentTimeStamp
        bucketfolderPath = "{}/{}/{}/{}".format(currentTimeStamp.year, currentTimeStamp.strftime("%B"),currentTimeStamp.day, currentTimeStamp.strftime("%H:%M:%S"))
        destinationBucketPath = "gs://{}/{}/{}/".format(BUCKET_NAME,"threatIntelCampaign",bucketfolderPath)
        # fetch todays and yesterday's date to pass campaign ID's api URL or it specify interval for /v2/campaign/campaign/ids  endpoint
        # pagination the page of results to return
        page=1
        #The maximum number of campaign IDs to produce in the response
        size=PAGE_SIZE
        # create empty list to append campaign dictionary from multiple pages
        campaignIdList=[]

        while True:
            try:
                logger.info("Fetching data cmapaign id's from /v2/campaign/ids endpoint")
                # update /v2/campaign/ids endpoint with page and size and interval
                campaignIdUrl = CAMPAIGN_ID_API_URL.format(dag_start_time,dag_end_time,page,size)
                logger.debug(campaignIdUrl)
                # fetch cmapaign id's from /v2/campaign/ids endpoint
                campaignIdResponse = requests.get(campaignIdUrl, auth=(principal, secret))
                # if response is 200 then extend capaignid's list if not break while loop
                logger.info("Checking response in /v2/campaign/ids endpoint")
                if campaignIdResponse.status_code == 200:
                    campaignIdDictionary = campaignIdResponse.json()
                    # check camapign list have
                    if len(campaignIdDictionary["campaigns"]) != 0:
                        campaignIdList.extend(campaignIdDictionary["campaigns"])
                        # increase page number
                        page=page+1
                    else:
                        break
                elif campaignIdResponse.status_code == 429:
                    logger.debug("The number of queries to this endpoint are limited by a simple, rolling 24-hour throttle. Once exceeded, the API will start returning 429 HTTP status codes until 24 hours past the oldest request has elapsed.")
                    break
                else:
                    break
            except requests.exceptions.HTTPError as error:
                if campaignIdResponse.status_code == 400:
                    logger.error(
                        "The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
                    logging.critical(
                        "The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
                    raise Exception("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
                elif campaignIdResponse.status_code == 401:
                    logger.error(
                        "There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
                    logger.critical(
                        "There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
                    raise Exception("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
                elif campaignIdResponse.status_code == 404:
                    logger.error("The campaign ID or threat ID does not exist.")
                    raise Exception("The campaign ID or threat ID does not exist.")
                elif campaignIdResponse.status_code == 500:
                    logger.error(
                        "The service has encountered an unexpected situation and is unable to give a better response to the request")
                    logger.critical(
                        "The service has encountered an unexpected situation and is unable to give a better response to the request")
                    raise Exception("The service has encountered an unexpected situation and is unable to give a better response to the request")
                else:
                    logger.error("Failed to fetch data from campaign api error is: {}".format(error))
                    logger.critical("Failed to fetch data from campaign api error is: {}".format(error))
                    raise Exception("Failed to fetch data from campaign api error is: {}".format(error))
            except Exception as error:
                logger.error("Failed to fetch data from /v2/campaign/ids endpoint error is : {}".format(error))
                raise Exception("Failed to fetch data from /v2/campaign/ids endpoint error is : {}".format(error))

        logger.info("Checking for number of campeign ids received")
        if len(campaignIdList)!=0:
            logger.info("Successfully fetched campaign Id's List: {}".format(campaignIdList))
            #ti.xcom_push(key="campaignIdList", value=campaignIdList)
        else:
            logger.info("we don't have campaign id's for processing")
            raise Exception("we don't have campaign id's for processing")
        Variable.set(key='list_campaign_id', value=campaignIdList, serialize_json=True)
        return destinationBucketPath
    except Exception as error:
        logger.error("Failed to fetch Id's from campaign api error is:{}".format(error))
        logging.critical("Failed to fetch Id's from campaign api error is:{}".format(error))
        raise AirflowException("Failed to fetch Id's from campaign api error is : {}".format(error))



def getDataFromCampaignAPI(campaignID,bucketfolderPath):
    """
        function will fetch campaign id's from campaignID api and then extract data for those campaign id's from campaign api
    """
    try:
            # append campaign id to campaign api url
            campaignUrl=CAMPAIGN_API_URL.format(campaignID["id"])
            logger.info("Fetching data for url {}".format(campaignUrl))
            # Fetch data from campaign api
            campaignResponse = requests.get(campaignUrl, auth=(principal, secret))
            if campaignResponse.status_code == 200:
                campaignDictionary = campaignResponse.json()
                # Extarct lastUpdatedAt from campaignID response append lastUpdatedAt to camapign api response
                lastUpdatedDate = {"lastUpdatedAt":campaignID["lastUpdatedAt"]}
                campaignDictionary.update(lastUpdatedDate)
                logger.info("Uploading data to bucket")
                uploadDataToBucket(campaignDictionary,"campaign_{}".format(campaignID["id"]),bucketfolderPath)
                logger.debug("campaign response uploaded for camapign id :{}".format(campaignID["id"]))
            #logger.info("threatIntel campaign DAG task update : successfully fetched data and uploaded to bucket for campaign id's : {}".format(campaignIdList))
    except requests.exceptions.HTTPError as error:
        if campaignResponse.status_code == 400:
            logger.error(
                "The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
            logging.critical(
                "The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
            raise Exception("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
        elif campaignResponse.status_code == 401:
            logger.error(
                "There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
            logger.critical(
                "There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
            raise Exception("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
        elif campaignResponse.status_code == 404:
            logger.error("The campaign ID or threat ID does not exist.")
            logger.crtical("The campaign ID or threat ID does not exist.")
            raise Exception("The campaign ID or threat ID does not exist.")
        elif campaignResponse.status_code == 500:
            logger.error(
                "The service has encountered an unexpected situation and is unable to give a better response to the request")
            logger.critical(
                "The service has encountered an unexpected situation and is unable to give a better response to the request")
            raise Exception("The service has encountered an unexpected situation and is unable to give a better response to the request")
        else:
            logger.error("Failed to fetch data from campaign api error is: {}".format(error))
            logger.critical("Failed to fetch data from campaign api error is: {}".format(error))
        raise AirflowException("Failed to fetch data from campaign api error is : {}".format(error))
    except Exception as error:
        logger.error("Failed to fetch data from campaign api error is:{}".format(error))
        logging.critical("Failed to fetch data from campaign api error is:{}".format(error))
        raise AirflowException("Failed to fetch data from campaign api error is : {}".format(error))

# def pullCampaignList(ti):
#     return ti.xcom_pull(key='campaignIdList', task_ids='extract_Id_from_campaign_api')

with DAG(
        dag_id="threatIntel_campaign_v1",
        tags=["threatIntel-campaign-DAG"],
        schedule_interval=SCHEDULE_INTERVAL,
        default_args=DEFAULT_DAG_ARGS,
        catchup=False,
) as dag:

    fetchId = python_operator.PythonOperator(
        task_id="extract_Id_from_campaign_api",
        python_callable=getIdFromCampaignAPI,
        op_kwargs={"dag_start_time": "{{ execution_date.subtract(days=1).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
                   "dag_end_time": "{{ execution_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}"},
        provide_context=True

        )


    dataflowDagCampaign = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_campaign",
        parameters={
                        "gcsBucketPath":"{{ task_instance.xcom_pull(task_ids='extract_Id_from_campaign_api', key='return_value') }}"
                    },
        template=CAMPAIGN_DATAFLOW_TEMPLATE_LOACATION,
        location=REGION,
        job_name="campaign"
    )

    campaignIdList = Variable.get(key='list_campaign_id',default_var=['campaign_id'],deserialize_json=True)
    # create dynamic operator for each campaign id to fetch data from campaign end point
    for campaign_id in campaignIdList:
        #convert string datatype to dict
        campaign_id=json.dumps(campaign_id)
        campaign_id=json.loads(campaign_id)
        dynamicTask = PythonOperator(
            task_id='campaign_id_{0}'.format(campaign_id["id"]),
            dag=dag,
            provide_context=True,
            python_callable=getDataFromCampaignAPI,
            op_kwargs={
                'bucketfolderPath': "{{ task_instance.xcom_pull(task_ids='extract_Id_from_campaign_api', key='return_value') }}",
                'campaignID': campaign_id})

        fetchId.set_downstream(dynamicTask)
        dynamicTask.set_downstream(dataflowDagCampaign)
    # start dummy operator
    opr_start = DummyOperator(task_id='START')
    opr_end = DummyOperator(task_id='END')

    # stream
    opr_start >> fetchId
    # opr_bridge >> dataflowDagCampaign
    dataflowDagCampaign >> opr_end