"""
DAG for Chronical Raw Ingestion
Description: dag will extract data from twao sources aws and office365, then load this data to two different bigquery tables.
And failed data files will be loaded in gcs bucket.

1. Get data from aws source and load it to bq
2. Get data from office365 and load it to bq

Dataflow Operator Flow:

1. Export data from AWS and office365
2. Check for status
3. Load data to bigquery tables

"""
#Importing dependancies
import json
import os
import json
import requests
import urllib
import time
import logging
import requests
import re
import gcsfs
from flatten_json import flatten
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient import _auth
from google.cloud import secretmanager, storage, bigquery
from airflow.decorators import dag, task

#Import Airflow dependencies and operators
from airflow import AirflowException #Airflow exception
from airflow.decorators import dag, task
from airflow import models,DAG
from airflow.operators import python_operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.models import Variable
from dependencies.logging_utils import setup_logger
from dependencies.yaml_reader_utils import config_reader
from dependencies.secret_manager_utils import get_secret
from dependencies.logging_utils import setup_logger

logger = setup_logger(logging.getLogger(__name__))
logger.info('Chronical Raw Ingestion pipeline execution started')


# read configuration parameters from custom config operator
category = "CHRONICAL_RAW_INGESTION"
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)

try:
    AWS_CLOUDTRAIL_DATASET = common_config_data["BIGQUERY"]["CHRONICAL_RAW"]["DATASET_ID"]
    AWS_CLOUDTRAIL_TABLE = common_config_data["BIGQUERY"]["CHRONICAL_RAW"]["CLOUDTRIAL_TABLE_ID"]
    OFFICE_365_DATASET = common_config_data["BIGQUERY"]["CHRONICAL_RAW"]["DATASET_ID"]
    OFFICE_365_TABLE = common_config_data["BIGQUERY"]["CHRONICAL_RAW"]["OFFICE365_TABLE_ID"]
    PROJECT_ID = common_config_data["GCP"]["PROJECT_ID"]
    SECRET_ID = config_data["SECRET_ID"]
    VERSION_ID = config_data["SECRET_VERSION"]
    BUCKET_NAME = config_data["BUCKET_NAME"]
    RETRIES = common_config_data["AIRFLOW"]["DEFAULT_DAG_ARGS_RETRIES"]
    START_DATE = common_config_data["AIRFLOW"]["DEFAULT_DAG_ARGS_START_DATE"]
    OWNER_NAME = common_config_data["AIRFLOW"]["DEFAULT_DAG_ARGS_OWNER"]
    DAG_PAST = common_config_data["AIRFLOW"]["DEFAULT_DAG_ARGS_DEPENDS_ON_PAST"]
    BUCKET_PATH = "projects/{}/buckets/{}".format(PROJECT_ID,BUCKET_NAME)
except (ValueError, KeyError) as error:
    logger.error("failed to read config variable, {}".format(error))
    raise KeyError("failed to read config variable, {}".format(error))


try:
    # read credential from secret manager
    SCOPES = ['https://www.googleapis.com/auth/chronicle-backstory']
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/{VERSION_ID}"
    response = client.access_secret_version(name=name)
    my_secret_value = response.payload.data.decode("UTF-8")
    my_secret_value = eval(my_secret_value)
    credentials = service_account.Credentials.from_service_account_info(my_secret_value, scopes=SCOPES)
    http_client = _auth.authorized_http(credentials)
except Exception as error:
    logger.error("Failed to fetch data from secret manager error is:{}".format(error))
    raise AirflowException("Failed to fetch data from secret manager error is:{}".format(error))


try:
    #Get Bigquery client
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID)
    logger.info("Bigquery connection created successfully.")
except Exception as error:
    logger.error("Failed to get bigquery client error is:{}".format(error))
    raise AirflowException("Failed to get bigquery client error is:{}".format(error))


DEFAULT_DAG_ARGS = {
    "owner": OWNER_NAME,
    "depends_on_past": False,
    "start_date": "2021-11-17",
    "retries": RETRIES,
}

def GetResponse(log_source, ti):
    """ GetResponse method checks status of data export job And it will wait until export is completed.
        Input:
            log_source: log source value it can be for aws cloudtrail or office365
            ti: task instance object.
    """
    try:
        logger.info("Check status of Data Export")
        # Initialize required parameters
        attempts, counter = 10, 1
        back_off = 60
        status = ""
        # Loop to check status continously until export job is finished
        while attempts >= counter and status != "FINISHED_SUCCESS":
            # Print check count
            logger.info("Checking status count: {}".format(counter))
            # Get data export Id
            dataexportid = ti.xcom_pull(key=log_source)
            logger.info("Processing for data export id:{}".format(dataexportid))
            # Prepare parameters for API Request
            url = "https://backstory.googleapis.com/v1/tools/dataexport/{}".format(dataexportid)
            method = "GET"
            # Make API Request Call
            response = http_client.request(url, method)
            logger.info("Api request response:{}".format(response))
            # Load json response object
            jsonObj = json.loads(response[1])
            status = jsonObj["dataExportStatus"]['stage']
            counter += 1
            # Increase delay exponentially
            delay = (counter * back_off) + 1
            logger.info('trying again in {} seconds'.format(delay))
            # Sleeping for seconds
            time.sleep(delay)
    except Exception as error:
        logger.error("Failed to fetch status of export job with error: {}".format(error))
        raise AirflowException("Failed to fetch status of export job with error: {}".format(error))


def GetDataexportid(log_source, dag_start_time, dag_end_time, ti):
    """Funtion fetches data export id
       Input:
            log_source: log source value it can be for aws cloudtrail or office365
            dag_start_time: Airflow dag start time val
            dag_end_time: Airflow dag end time val
            ti: task instance object val
    """
    try:
        # ti = kwargs['ti']
        # Initialize required parameters for API call
        url = "https://backstory.googleapis.com/v1/tools/dataexport"
        method = "POST"
        print(dag_start_time, "  ", dag_end_time)
        # Prepare paylaod for API call request
        data_string = {"logType": log_source,
                       "gcsBucket": "projects/njccic-sdw-dev/buckets/njccic-dataexport",
                       "startTime": dag_start_time,
                       "endTime": dag_end_time}
        # Dump payload into json format
        data = json.dumps(data_string)
        # Prepare header for request call
        headers = {"Content-Type": "application/json"}
        # Make API call request
        response = http_client.request(url, method, data, headers)
        # Get response into json format
        jsonObj = json.loads(response[1])
        print(jsonObj)
        dataexportid = jsonObj["dataExportId"]
        logger.info("dataexport id {}".format(dataexportid))
        # GetResponse(dataexportid)
        # Push log source value to next task
        ti.xcom_push(key=log_source, value=dataexportid)
        # Variable.set(log_source, dataexportid)
        Variable.set(key=log_source, value=dataexportid, serialize_json=True)
        # Variable.set(key="AWS_CLOUDTRAIL", value=dataexportid, serialize_json=True)
        # Variable.set(key="OFFICE_365", value=dataexportid, serialize_json=True)
    except Exception as error:
        logger.error("Failed to fetch data export id with error {}".format(error))
        raise AirflowException("Failed to fetch data export id with error {}".format(error))


def schema_evolution(log_source,blob_name, ti):
    """Schema evolution function reads data from gcs and loads this data to bigquery.
        Input:
            log_source: log source value it can be for aws cloudtrail or office365
            ti: Task instance object b
    """
    try:
        logger.info("Processing to schema evolution for {}".format(log_source))
        # Get data export id
        dataexportid = ti.xcom_pull(key=log_source)
        # Get bigquery table and dataset reference.
        dataset_ref = bigquery.DatasetReference(bq_client.project, AWS_CLOUDTRAIL_DATASET)
        table_ref = bigquery.TableReference(dataset_ref, AWS_CLOUDTRAIL_TABLE)

        logger.info("Reading from blob files")
        # Process each blob one by one
        logger.debug("Processing blob:{}".format(blob_name))
        # Get gcs json path for blob file
        gcs_json_path = f"gs://{BUCKET_NAME}/{blob_name}"
        # Get gcs object to read files
        gcs_file_system = gcsfs.GCSFileSystem(project=PROJECT_ID)
        # Read from json file
        with gcs_file_system.open(gcs_json_path) as f:
            logger.debug("File opened:{}".format(gcs_json_path))
            # Fetch bigquery table
            bigquery_table = bq_client.get_table(table_ref)
            # Get table schema
            org_schema = bigquery_table.schema
            new_schema = org_schema[:]
            # Initiate required parameters for schema evolution
            test_schema = "schema_table_test"
            valid_row = []
            json_schema = []
            bq_lt_schema = ["{0}".format(schema.name).lower() for schema in bigquery_table.schema]
            # print(bq_lt_schema)
            to_add = bq_lt_schema
            # Prepare new schema if their are schema changes
            for index,line in enumerate(f):
                print(index ," : ",blob_name)
                try:
                    # Load json data
                    data_json = json.loads(line, strict=False)
                    clean_json = data_json
                    flatdata = flatten(clean_json)
                    json_schema = []
                    flatdata_new = []
                    # Prepare Key values for schema
                    for k,v in flatdata.items():
                        pos = k.split("_",2)[1]
                        k_new = k.replace(".", "_")
                        k_new = re.sub(":", "_", k_new)
                        k_new = re.sub("-", "_", k_new)
                        k_new = re.sub(r"^Records_\d*_", "", k_new)
                        if isinstance(v, list) and len(v) == 0:
                            flatdata[k] = ""
                        if isinstance(v, dict) and len(v) == 0:
                            flatdata[k] = ""
                        if v == True:
                            flatdata[k] = "true"
                        if v == None:
                            flatdata[k] = ""
                        try:  # If key does not exists
                            flatdata_new[int(pos)][k_new] = flatdata[k]
                        except Exception as e:
                            # Insert new key
                            flatdata_new.insert(int(pos),{k_new:flatdata[k]})
                    # Appends values to schema
                    for json_key in flatdata.keys():
                        old_key = json_key
                        json_key = json_key.replace(".", "_")
                        json_key = re.sub(":", "_", json_key)
                        json_key = re.sub("-", "_", json_key)
                        json_key = re.sub(r"^Records_\d*_", "", json_key)
                        json_schema.append(json_key.lower())
                    delta = list(set(json_schema).difference(set(to_add)))
                    table_id = "{}.{}.{}".format(PROJECT_ID,AWS_CLOUDTRAIL_DATASET,test_schema)
                    if len(delta) != 0:
                        for new_col in delta:
                            new_schema.append(bigquery.SchemaField(new_col, "STRING", mode="NULLABLE"))
                            to_add.append(new_col)
                        print("Successfully added column in {0} table".format(bigquery_table))
                    valid_row += flatdata_new
                except Exception as e:
                    logger.error(e)
                    print(str(e))
            logger.info("Modifying schema of bigquery table")
            # Load data to final bigquery table
            bigquery_table.schema = new_schema # Assign new schema
            bq_client.update_table(bigquery_table, ['schema']) #Update table schema
            logger.info("Load data into bigquery table")
            # Set bigquery job config values
            bq_schema = bigquery_table.schema
            job_config = bigquery.job.LoadJobConfig()
            # Get bq schema
            job_config.schema = bq_schema
            # Set source format
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            # Set bq load mode
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            # call method to load data into bigquery
            job = bq_client.load_table_from_json(valid_row, table_id, job_config=job_config)
            job.result()
        logging.info("Schema Evolution completed.")
    except Exception as error:
        logger.error("Failed to do schema evolution with error:{}".format(error))
        raise AirflowException("Failed to do schema evolution with error:{}".format(error))

def schema_evolution_office365(log_source, blob_name, ti):
    """Schema evolution function reads data from gcs and loads this data to bigquery.
        Input:
            log_source: log source value it can be for aws cloudtrail or office365
            ti: Task instance object b
    """
    try:
        logger.info("Processing to schema evolution for {}".format(log_source))
        # Get data export id
        dataexportid = ti.xcom_pull(key=log_source)
        # Get list of files to read data from.
        # blobs = storage_client.list_blobs(BUCKET_NAME, prefix=dataexportid)

        print("Blobs:")
        # Get bigquery table and dataset reference.
        dataset_ref = bigquery.DatasetReference(bq_client.project, OFFICE_365_DATASET)
        table_ref = bigquery.TableReference(dataset_ref, OFFICE_365_TABLE)

        logger.info("Reading from blob files")
        logger.debug("Processing blob:{}".format(blob_name))
        # Get gcs json path for blob file
        gcs_json_path = f"gs://{BUCKET_NAME}/{blob_name}"
        # Get gcs object to read files
        gcs_file_system = gcsfs.GCSFileSystem(project=PROJECT_ID)
        # Read from json file
        with gcs_file_system.open(gcs_json_path) as f:
            logger.debug("File opened:{}".format(gcs_json_path))
            # Fetch bigquery table
            bigquery_table = bq_client.get_table(table_ref)
            # Get table schema
            org_schema = bigquery_table.schema
            new_schema = org_schema[:]
            # Initiate required parameters for schema evolution
            test_schema = "schema_table_test"
            valid_row = []
            json_schema = []
            bq_lt_schema = ["{0}".format(schema.name).lower() for schema in bigquery_table.schema]
            # print(bq_lt_schema)
            to_add = bq_lt_schema
            # Prepare new schema if their are schema changes
            for index,line in enumerate(f):
                print(index ," : ",blob_name)
                try:
                    # Load json data
                    data_json = json.loads(line, strict=False)
                    clean_json = data_json
                    flatdata = flatten(clean_json)
                    json_schema = []
                    flatdata_new = []
                    # Prepare Key values for schema
                    for k,v in flatdata.items():
                        pos = k.split("_",2)[1]
                        k_new = k.replace(".", "_")
                        k_new = re.sub(":", "_", k_new)
                        k_new = re.sub("-", "_", k_new)
                        k_new = re.sub(r"^Records_\d*_", "", k_new)
                        if isinstance(v, list) and len(v) == 0:
                            flatdata[k] = ""
                        if isinstance(v, dict) and len(v) == 0:
                            flatdata[k] = ""
                        if v == True:
                            flatdata[k] = "true"
                        if v == None:
                            flatdata[k] = ""
                        try:  # If key does not exists
                            flatdata_new[int(pos)][k_new] = flatdata[k]
                        except Exception as e:
                            # Insert new key
                            flatdata_new.insert(int(pos),{k_new:flatdata[k]})
                    # Appends values to schema
                    for json_key in flatdata.keys():
                        old_key = json_key
                        json_key = json_key.replace(".", "_")
                        json_key = re.sub(":", "_", json_key)
                        json_key = re.sub("-", "_", json_key)
                        json_key = re.sub(r"^Records_\d*_", "", json_key)
                        json_schema.append(json_key.lower())
                    # Prepare Schema values
                    delta = list(set(json_schema).difference(set(to_add)))
                    table_id = "{}.{}.{}".format(PROJECT_ID,OFFICE_365_DATASET,test_schema)
                    if len(delta) != 0:
                        for new_col in delta:
                            new_schema.append(bigquery.SchemaField(new_col, "STRING", mode="NULLABLE"))
                            to_add.append(new_col)
                        print("Successfully added column in {0} table".format(bigquery_table))
                    valid_row += flatdata_new
                except Exception as e:
                    logger.error(e)
                    print(str(e))
            logger.info("Modifying schema of bigquery table")
            # Load data to final bigquery table
            bigquery_table.schema = new_schema # Assign new schema
            bq_client.update_table(bigquery_table, ['schema']) #Update table schema
            logger.info("Load data into bigquery table")
            # Set bigquery job config values
            bq_schema = bigquery_table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            # call method to load data into bigquery
            job = bq_client.load_table_from_json(valid_row, table_id, job_config=job_config)
            job.result()
        logging.info("Schema Evolution completed.")
    except Exception as error:
        logger.error("Failed to do schema evolution with error:{}".format(error))
        raise AirflowException("Failed to do schema evolution with error:{}".format(error))

def get_files_to_process(log_source,dataexportid):
    """Reads files from bucket for given log source and returns list of files.
        Input:
            log_source: log source value it can be for aws cloudtrail or office365
            dataexportid: data export id value to read from bucket
        Output
            files_to_process: List of file names
    """
    try:
        logger.info("Fetching files to process from {}".format(log_source))
        #Read list of files from bucket
        blobs = storage_client.list_blobs(BUCKET_NAME, prefix=dataexportid)
        files_to_process = []
        #Filter file list for extension
        for blob in blobs:
            if re.match(r"^.*\.(csv)$", blob.name):
                files_to_process.append(blob.name)
        logger.info("Successfully fetched files from bucket")
        return files_to_process
    except Exception as error:
        logger.error("Failed to process files from bucket with error:()".format(error))
        raise AirflowException("Failed to process files from bucket with error:()".format(error))

# Initializing counters for dynamic task creation
aws_cnt=0
office365_cnt=0
# Dag flow declaratio
with DAG(
        dag_id="chronicle_ingestion_test_v2",
        schedule_interval="@daily",
        default_args=DEFAULT_DAG_ARGS,
        catchup=False
) as dag:
    # Star Operator
    opr_start = DummyOperator(task_id='START')

    aws_cloudtrail_export = python_operator.PythonOperator(
        task_id="aws_cloudtrail_gcs_export",
        python_callable=GetDataexportid,
        op_kwargs={
            "log_source": "AWS_CLOUDTRAIL",
            "dag_start_time": "{{ execution_date.subtract(days=1).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "dag_end_time": "{{ execution_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}"
        })

    office_365_export = python_operator.PythonOperator(
        task_id="office_365_gcs_export",
        python_callable=GetDataexportid,
        op_kwargs={
            "log_source": "OFFICE_365",
            "dag_start_time": "{{ execution_date.subtract(days=1).strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "dag_end_time": "{{ execution_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}"
        })

    aws_cloudtrail_job_status = python_operator.PythonOperator(
        task_id="aws_cloudtrail_check_job_status",
        python_callable=GetResponse,
        op_kwargs={
            "log_source": "AWS_CLOUDTRAIL"
        }
    )

    office_365_job_status = python_operator.PythonOperator(
        task_id="office_365_check_job_status",
        python_callable=GetResponse,
        op_kwargs={
            "log_source": "OFFICE_365"
        }
    )
    # End operator
    opr_end = DummyOperator(task_id='END')
    # Get List of files to create task dynamically
    files_to_process_list_aws = get_files_to_process("AWS_CLOUDTRAIL", Variable.get(key="AWS_CLOUDTRAIL", default_var=['AWS_CLOUDTRAIL'], deserialize_json=True))
    files_to_process_list_office365 = get_files_to_process("OFFICE_365", Variable.get(key="OFFICE_365", default_var=['OFFICE_365'], deserialize_json=True))

    for file in files_to_process_list_aws:
        op_aws = python_operator.PythonOperator(
                task_id='process_file_aws_{}'.format(aws_cnt),
                python_callable=schema_evolution,
                op_kwargs={"log_source": "AWS_CLOUDTRAIL","blob_name":file}
            )
        aws_cnt+=1
        aws_cloudtrail_job_status.set_downstream(op_aws)
        op_aws.set_downstream(opr_end)

    for file in files_to_process_list_office365:
        op_office365 = python_operator.PythonOperator(
                task_id='process_file_office365_{}'.format(office365_cnt),
                python_callable=schema_evolution_office365,
                op_kwargs={"log_source": "OFFICE_365","blob_name":file}
            )
        office365_cnt+=1
        office_365_job_status.set_downstream(op_office365)
        op_office365.set_downstream(opr_end)


    opr_start >> [aws_cloudtrail_export,office_365_export]
    aws_cloudtrail_export >> aws_cloudtrail_job_status >> opr_end
    office_365_export >> office_365_job_status >> opr_end

