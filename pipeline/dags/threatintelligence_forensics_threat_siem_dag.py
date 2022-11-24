"""
DAG for threatintel api's - siem, forensics, threat
Description: dag will extract data from siem, forensics, threat api will upload it to gcs bucket and trigger dataflow pipelines
Python Operator Flow:

1. Extract data from siem api
2. Fetch threatID's from siem api response
3. Extract data for those ThreatID's from forensics,threat api
4. upload data to gcs bucket

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

#from dependencies.logger_utils import setup_logger
from dependencies.logging_utils import setup_logger
from dependencies.yaml_reader_utils import config_reader
from dependencies.secret_manager_util import get_secret
from airflow.decorators import task
from airflow.models import Variable

from datetime import datetime, timezone, timedelta,time

from requests.auth import HTTPDigestAuth

import json
import logging
import requests


logger = setup_logger(logging.getLogger(__name__))
logger.info('ThreatIntel pipeline execution started')

category = "THREAT_INTELLIGENCE"
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)

try:
    PROJECT_ID = common_config_data['GCP']['PROJECT_ID']
    REGION = common_config_data['GCP']['REGION']
    SECRET_ID = config_data['SECRETMANAGER_SECRET_ID']
    SCHEDULE_INTERVAL = config_data['HOURLY_SCHEDULE_INTERVAL']
    BUCKET_NAME = config_data['THREAT_INTELLIGENCE_API_OUTPUT_BUCKET']
    SECRET_VERSION_ID = config_data['SECRET_VERSION']
    DAG_START_DATE = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']
    DAG_OWNER = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    DAG_RETRIES = common_config_data['AIRFLOW'] ['DEFAULT_DAG_ARGS_RETRIES']
    THREAT_API_URL = config_data['THREAT_API_URL']
    SIEM_API_URL = config_data['SIEM_API_URL']
    FORENSICS_API_URL = config_data['FORENSICS_API_URL']
    THREAT_DATAFLOW_TEMPLATE_LOACATION = config_data["DATAFLOW_TEMPLATE_BUCKET_PATH_THREAT"]
    FORENSICS_DATAFLOW_TEMPLATE_LOACATION = config_data["DATAFLOW_TEMPLATE_BUCKET_PATH_FORENSICS"]
    SIEMCLICKS_DATAFLOW_TEMPLATE_LOACATION = config_data["DATAFLOW_TEMPLATE_BUCKET_PATH_SIEMCLICK"]
    SIEMMESSAGE_DATAFLOW_TEMPLATE_LOACATION = config_data["DATAFLOW_TEMPLATE_BUCKET_PATH_SIEMMESSAGE"]

except (ValueError, KeyError) as error:
    logger.error("failed to read config variable, %s", error)
    raise KeyError(f"failed to read config variable, {error}")

try:
    # read credential from secret manager
    logger.info("Fetching data from secret manager.")
    secretApiKey = json.loads(get_secret(PROJECT_ID, SECRET_ID, SECRET_VERSION_ID))
    principal = secretApiKey['principal']
    secret = secretApiKey['secret']
    logger.info("Successfully  fetched data from secret manager.")

except Exception as error:
    logger.error("Failed to fetch data from secret manager error is:{}".format(error))
    raise AirflowException("Failed to fetch data from secret manager error is:{}".format(error))

# These DEFAULT_DAG_ARGS will get passed on to each operator
# You can override them on a per-task basis during operator initialization
DEFAULT_DAG_ARGS = {
    "start_date": DAG_START_DATE,
    'owner': DAG_OWNER,
    'retries': DAG_RETRIES,  # re-run scenario
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
}



def uploadDataToBucket(apiData, apiType, bucketfolderPath):
    """
        api response data will upload to gcs bucket
        --input parameters:
            apiData : contain response of siem or threat or forensics api
            apiType : contain api type for e.g siem or forensics or threat
            bucketfolderPath : destination folder path
    """
    try:
        # Destination gcs folder path
        logger.info("create destination gcs bucket folder path")
        prefix=bucketfolderPath.split("/", 3)[3]
        destinationPath = "{}{}.json".format(prefix, apiType)
        logger.debug("data uploadation start at bucket path is:gs://{}/{}".format(
            BUCKET_NAME, destinationPath))
        #create gcs bucket client
        storage_client = storage.Client()
        # Retrieve a bucket using bucket name
        threatIntelBucket = storage_client.get_bucket(BUCKET_NAME)
        threatIntelBlob = threatIntelBucket.blob(destinationPath)
        #upload api response to bucket
        threatIntelBlob.upload_from_string(json.dumps(apiData))
        logger.info("Uploaded data to bukcet on location:{}".format(destinationPath))

    except Exception as error:
        logger.error("Failed to upload  api response to bucket Error is:{}".format(error))
        logger.critical("Failed to upload  api response to bucket Error is:{}".format(error))
        raise AirflowException(f"Failed to upload  api response to bucket Error is: {error}")


def fetchDataFromApi(threatID, apiUrl, bucketFolderpath, apiType):
    """
     function will fetch data from forensics and threat api
     --input parameters
         apiURL : contain api url for threat, forensics depends on function will call for which api
         threatIDList : contain list of threatID's
         apiType : fetchDataFromApi function will call for which api e.g api call for forensics then apiType= forensics
         bucketFolderpath : destination bucket folder path
     """
    try:
            # add threatID to apiURL
            apiUrl = apiUrl.format(threatID)
            # fetch data from api
            responseAPI = requests.get(apiUrl, auth=(principal, secret))
            # fetching data from api
            logger.info(responseAPI.status_code)
            if responseAPI.status_code == 200:
                apiResponse = responseAPI.json()
                # upload data to gcs bucket
                uploadDataToBucket(apiResponse, "{}{}".format(apiType, threatID), bucketFolderpath)
            elif responseAPI.status_code == 429:
                logger.error("The number of queries to this endpoint are limited by a simple, rolling 24-hour throttle. Once exceeded, the API will start returning 429 HTTP status codes until 24 hours past the oldest request has elapsed.")
    # return [data]
    except requests.exceptions.HTTPError as error:
        if responseAPI.status_code == 400:
            logger.error("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
            logger.critical("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
            raise Exception("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
        elif responseAPI.status_code == 401:
            logger.error("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
            logger.critical("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
            raise Exception("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")

        elif responseAPI.status_code == 404:
            logger.error("The campaign ID or threat ID does not exist.")
            logger.critical("The campaign ID or threat ID does not exist.")
            raise Exception("The campaign ID or threat ID does not exist.")
        elif responseAPI.status_code == 500:
            logger.error("The service has encountered an unexpected situation and is unable to give a better response to the request")
            logger.critical("The service has encountered an unexpected situation and is unable to give a better response to the request")
            raise Exception("The service has encountered an unexpected situation and is unable to give a better response to the request")

        else:
            logger.error("Failed to fetch data from api error is: {}".format(error))
            logger.critical("Failed to fetch data from api error is: {}".format(error))
            raise Exception("Failed to fetch data from api error is: {}".format(error))
    except Exception as error:
        logger.debug("Failed to fetch data from threat or forensics  api error is:{}".format(error))
        raise


def getDataFromSeimAPI():
    """function will fetch siem api data then extract threatID's from siem response will call forensics, threat api"""
    try:
        logger.info("fetch siem api data")
        # fetch current timestamp in UTC
        destinationBucketPath = None
        currentTimeStamp = datetime.now(timezone.utc)
        # currentTimeStamp.year : fetch year from currenttimestamp , currentTimeStamp.strftime("%B") : fetch month from currentTimeStamp ,currentTimeStamp.strftime("%H:%M:%S") : fetch hour from currentTimeStamp
        bucketfolderPath = "{}/{}/{}/{}".format(currentTimeStamp.year,currentTimeStamp.strftime("%B"),currentTimeStamp.day,currentTimeStamp.strftime("%H:%M:%S"))
        destinationBucketPath="gs://{}/{}/{}/".format(BUCKET_NAME,"threatIntel",bucketfolderPath)
        logger.debug("extracting data from siem api")
        responseSiemAPI = requests.get(SIEM_API_URL, auth=(principal, secret))
        logger.debug("data successfully fetched from siem api")
        # convert response to dictionary format
        siemDictionary  = responseSiemAPI.json()
        #upload siem api response to bucket
        uploadDataToBucket(siemDictionary, "siem", bucketfolderPath)
        logger.debug("Siem api response uploaded successfully to bucket")

        #make empty list for to append threadID's to threat list
        threatIDList = []
        logger.debug("Making thraetID's list")
        # fetch threatID's from messagesBlocked list
        # convert dictionary keys to lowercase
        siemDictionary={key.lower(): value for key, value in siemDictionary.items()}


        for messagesBlockedList in siemDictionary["messagesblocked"]:
            #covert messagesBlockedList keys to lower case
            messagesBlockedList = {key.lower(): value for key, value in messagesBlockedList.items()}
            for threatsInfoMap in messagesBlockedList["threatsinfomap"]:
                threatsInfoMap = {key.lower(): value for key, value in threatsInfoMap.items()}
                if threatsInfoMap["threatid"] != None:
                    threatIDList.append(threatsInfoMap["threatid"])

        #fetch threatID's from messagesDelivered list
        for messagesDeliveredList in siemDictionary["messagesdelivered"]:
            #covert messagesDeliveredList keys to lower case
            messagesDeliveredList = {key.lower(): value for key, value in messagesDeliveredList.items()}
            for threatsInfoMap in messagesDeliveredList["threatsinfomap"]:
                # covert threatsInfoMap keys to lower case
                threatsInfoMap = {key.lower(): value for key, value in threatsInfoMap.items()}
                # check threatID is not null
                if threatsInfoMap["threatid"] != None:
                    # if threatID is not null then append threatId to threatIDList
                    threatIDList.append(threatsInfoMap["threatid"])

        # fetch threatID's from clicksBlocked list
        for clicksBlocked in siemDictionary["clicksblocked"]:
            # covert clicksBlocked keys to lower case
            clicksBlocked = {key.lower(): value for key, value in clicksBlocked.items()}
            # check threatID is not null
            if clicksBlocked["threatid"] != None:
                #if threatID is not null then append threatId to threatIDList
                threatIDList.append(clicksBlocked["threatid"])

        for clicksPermitted in siemDictionary["clickspermitted"]:
            # covert clicksPermitted keys to lower case
            clicksPermitted = {key.lower(): value for key, value in clicksPermitted.items()}
            # check threatID is not null
            if clicksPermitted["threatid"] != None:
                #if threatID is not null then append threatId to threatIDList
                threatIDList.append(clicksPermitted["threatid"])

        logger.debug("Count of threat id's before dublication removal :{}".format(len(threatIDList)))
        logger.debug("before duplication remove from threat id's list:{}".format(threatIDList))
        # to remove duplicated ID's from threatIDList
        threatIDList=list(set(threatIDList))
        logger.debug("after duplication remove from threat id list : {}".format(threatIDList))
        logger.debug("Count of threat id's after dublication removal :{}".format(len(threatIDList)))
        logger.debug("extracting data from threat api")
        # call fetchDataFromApi function for threat api
        #fetchDataFromApi(threatIDList,THREAT_API_URL,bucketfolderPath,"threat_")
        logger.debug("Data extracted and uploaded to bucket successfully for threat api")
        logger.debug("extracting data from forensics api")
        # call fetchDataFromApi function for forensics api
        #fetchDataFromApi(threatIDList,FORENSICS_API_URL,bucketfolderPath,"forensics_")
        logger.debug("Data extracted and uploaded to bucket successfully for forensics api")
        logger.info("Data successfully uploaded to bucket for forensic and threat and siem")
        Variable.set(key='list_threat_id', value=threatIDList, serialize_json=True)

    except requests.exceptions.HTTPError as error:
        if responseSiemAPI.status_code == 400:
            logger.error("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
            logger.critical("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")
            raise Exception("The request is missing a mandatory request parameter, a parameter contains data which is incorrectly formatted, or the API doesn't have enough information to determine the identity of the customer.")

        elif responseSiemAPI.status_code == 401:
            logger.error("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
            logger.critical("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")
            raise Exception("There is no authorization information included in the request, the authorization information is incorrect, or the user is not authorized")

        elif responseSiemAPI.status_code == 404:
            logger.error("The campaign ID or threat ID does not exist.")
            logger.critical("The campaign ID or threat ID does not exist.")
            raise Exception("The campaign ID or threat ID does not exist.")
        elif responseSiemAPI.status_code == 500:
            logger.error("The service has encountered an unexpected situation and is unable to give a better response to the request")
            logger.critical("The service has encountered an unexpected situation and is unable to give a better response to the request")
            raise Exception("The service has encountered an unexpected situation and is unable to give a better response to the request")
        else:
            logger.error("Failed to fetch data from api error is: {}".format(error))
            logger.critical("Failed to fetch data from api error is: {}".format(error))
            raise Exception("Failed to fetch data from api error is: {}".format(error))
    except Exception as error:
        logger.error("Failed to fetch data from api error is:{}".format(error))
        logger.critical("Failed to fetch data from api error is:{}".format(error))
        raise AirflowException("Failed to fetch data from  api error is : {}".format(error))

    logger.debug("gcs bucket files path : {}".format(destinationBucketPath))

    return destinationBucketPath

with DAG(
        dag_id="threatIntel_siem_forensics_threat_v1",
        tags=["threatIntel_DAG"],
        schedule_interval=SCHEDULE_INTERVAL,
        default_args=DEFAULT_DAG_ARGS,
        catchup=False,
) as dag:

    #start dummy operator

    fetchId = python_operator.PythonOperator(
        task_id="extract_data_from_threatintel_api",
        python_callable=getDataFromSeimAPI)
    #dummy operator for threat end
    threat_end = DummyOperator(task_id='threat_end')
    #dummy task operator for forensic end
    forensic_end = DummyOperator(task_id='forensics_end')
    # dataflow operator for threat
    dataflowDagThreat = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_threat",
        parameters={
                        "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='extract_data_from_threatintel_api', key='return_value') }}"
                    },
        template=THREAT_DATAFLOW_TEMPLATE_LOACATION,
        location=REGION,
        job_name="threat"
    )
    # dataflow operator for forensics
    dataflowDagForensic = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_forensics",
        parameters={
            "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='extract_data_from_threatintel_api', key='return_value') }}"
        },
        template=FORENSICS_DATAFLOW_TEMPLATE_LOACATION,
        location=REGION,
        job_name="forensics"
    )
    dataflowDagSiemClick = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_siem_click",
        parameters={
            "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='extract_data_from_threatintel_api', key='return_value') }}"
        },
        template=SIEMCLICKS_DATAFLOW_TEMPLATE_LOACATION,
        location=REGION,
        job_name="siem_click"
    )
    dataflowDagSiemMessage = DataflowTemplatedJobStartOperator(
        task_id="dataflow_run_for_siem_message",
        parameters={
            "gcsBucketPath": "{{ task_instance.xcom_pull(task_ids='extract_data_from_threatintel_api', key='return_value') }}"
        },
        template=SIEMMESSAGE_DATAFLOW_TEMPLATE_LOACATION,
        location=REGION,
        job_name="siem_message"
    )
    #get list of threat id's from variables
    threatIdList = Variable.get(key='list_threat_id', default_var=['threat_id'], deserialize_json=True)
    #dynamically create operator for each threat id to fetch data from forensics api
    for threat_id in threatIdList:
        # i = campaign_id["id"]
        threat_id = json.dumps(threat_id)
        threat_id = json.loads(threat_id)
        dynamicTaskForensics = PythonOperator(
            task_id='forensics_id_{0}'.format(threat_id),
            dag=dag,
            provide_context=True,
            python_callable=fetchDataFromApi,
            op_kwargs={
                'bucketFolderpath': "{{ task_instance.xcom_pull(task_ids='extract_data_from_threatintel_api', key='return_value') }}",
                'threatID': threat_id,'apiType':"forensics_",'apiUrl': FORENSICS_API_URL})

        fetchId.set_downstream(dynamicTaskForensics)
        dynamicTaskForensics.set_downstream(forensic_end)

    #dynamically create operator for each threat id to fetch data from threat api
    for threat_id in threatIdList:
        # i = campaign_id["id"]
        threat_id = json.dumps(threat_id)
        threat_id = json.loads(threat_id)
        dynamicTaskThreat = PythonOperator(
            task_id='threat_id_{0}'.format(threat_id),
            dag=dag,
            provide_context=True,
            python_callable=fetchDataFromApi,
            op_kwargs={
                'bucketFolderpath': "{{ task_instance.xcom_pull(task_ids='extract_data_from_threatintel_api', key='return_value') }}",
                'threatID': threat_id,'apiType':"threat_",'apiUrl': THREAT_API_URL})

        forensic_end.set_downstream(dynamicTaskThreat)
        dynamicTaskThreat.set_downstream(threat_end)
        threat_end.set_downstream(dataflowDagForensic)
        threat_end.set_downstream(dataflowDagThreat)
        threat_end.set_downstream(dataflowDagSiemClick)
        threat_end.set_downstream(dataflowDagSiemMessage)

    opr_start = DummyOperator(task_id='START')
    opr_end = DummyOperator(task_id='END')

    # stream
    opr_start >> fetchId
    # opr_bridge >> dataflowDagCampaign
    [dataflowDagSiemMessage,dataflowDagForensic,dataflowDagThreat,dataflowDagSiemClick] >> opr_end