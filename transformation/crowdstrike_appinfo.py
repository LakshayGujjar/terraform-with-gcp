"""
dataflow for aidmaster api

Description: Dataflow will read data from gcs bucket, unzips data, transforms data and then ingest data to bigquery.

Steps:
1. Make list of aidmaster file gcs bucket path
2. Unzip aidmaster data file
3. Read aidmaster file data
4. Transform data
5. Ingest data to bigquery
#
"""

import json
import gcsfs
import gzip
import requests
import time
import yaml
import argparse, logging
import apache_beam as beam
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import secretmanager
from yaml.loader import SafeLoader
import datetime
from datetime import timezone
import pandas as pd
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class CustomParam(PipelineOptions):
    """ You can add your own custom parameter options in addition to the standard PipelineOptions.
        we have added gcsBucketPath as custom parameter.
        gcsBucketPath : gcs bucket folder path which contain files """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--gcsBucket', dest='gcsBucket', required=False, help='Bucket name')


class getBucketFilesList(beam.DoFn):
    """ Fetch aidmaster file path from bucket
        Lists all the blobs in the bucket that begin with the prefix.

        This can be used to list all blobs in a "folder", e.g. "threatIntel/".

        As part of the response, you'll also get back a blobs.prefixes entity
    """
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, element, custom_options):
        try:
            # Get storage client
            from google.cloud import storage
            client = storage.Client()
            bucketPathList = []
            # Fetch custom input value which contain GCS bucket folder path of aidmaster files
            bucketPath = str(custom_options.gcsBucket.get())
            # Extract bucket name from input
            bucketName = bucketPath.split("/",3)[2]
            # Extract folder in the bucket that begin with the prefix
            prefix = bucketPath.split("/",3)[3]
            # Note: The call returns a response only when the iterator is consumed.
            for blob in client.list_blobs(bucketName, prefix=prefix):
               bucketPathList.append(blob.name)
            logging.info("bucket list:{}".format(bucketPathList))
            return bucketPathList # Returns bucket path list for aidmaster.
        except Exception as e:
            logging.error("Failed to get list of gcs files path error is : {}".format(e))
            raise Exception(e)

    

class transformation(beam.DoFn):
    """ Reads data from GCS bucket, transforms data into required format
    """
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)
    

    def process(self, bucketPath,custom_options,projectId):
        def readDataFromBucket(gcsJsonPath,projectId):
            """ Reads data from input path. Unzips input file and returns json dictionary list.
                Input Parameters:
                    gcsJsonPath<String> - Gcs path for input file
                Output:
                    Returns fileDictionaryList<Dictionary> Object contains file data.
            """
            try:
                logging.info("Reading data from GCS bucket")
                import gcsfs
                # fs = GCSFileSystem(project='my-google-project', session_kwargs={'trust_env': True})
                gcsFileSystem = gcsfs.GCSFileSystem(project = projectId, token='cloud',session_kwargs={'trust_env': True}) #Get GCS connection
                fileDictionaryList = []
                logging.debug("Reading data for file {}".format(gcsJsonPath))
                # Read input zip file
                with gcsFileSystem.open(gcsJsonPath) as f: 
                    gzipFileData = gzip.GzipFile(fileobj=f)
                    # Unzip and load it into dictionary object
                    for element in gzipFileData: 
                        fileDictionary = json.loads(element)
                        fileDictionaryList.append(fileDictionary)
                return fileDictionaryList # Returns dictionary list object
            except Exception as e:
                logging.error("Failed to read data from bucket with error {}".format(e))
                raise Exception(e)

        try:
            #fetch custom input value which contain GCS bucket folder path of threatIntel files
            path = str(custom_options.gcsBucket.get())
            # Get bucket name
            bucketName = path.split("/",3)[2]
            # Read data from bucket
            appinfoDictList = readDataFromBucket("gs://{}/{}".format(bucketName,bucketPath),projectId)
            logging.info("Data read completed. Moving on to transformations")
            # Create dic list to store transformed rows
            appinfoTransformationObjList=[]
            lastSeenUTC = None
            cid = None
            companyName =None
            detectionCount = None
            eventPlatform = None
            fileDescription = None
            fileName = None
            fileVersion = None
            productName = None
            productVersion =None
            sha256HashData = None
            for appinfoDict in appinfoDictList:
                # convert dictionary keys to lower case
                appinfoDict = {key.lower(): value for key, value in appinfoDict.items()}
                # check _time is present in appinfoDict dictionary
                if "_time" in appinfoDict:
                    lastSeenUTC = appinfoDict["_time"]
                # check cid is present in appinfoDict dictionary
                if "cid" in appinfoDict:
                    cid = appinfoDict["cid"]
                if "companyname" in appinfoDict:
                    companyName = appinfoDict["companyname"]
                if "detectioncount" in appinfoDict:
                    detectionCount = appinfoDict["detectioncount"]
                if "event_platform" in appinfoDict:
                    eventPlatform = appinfoDict["event_platform"]
                if "filedescription" in appinfoDict:
                    fileDescription = appinfoDict["filedescription"]
                if "fileName" in appinfoDict:
                    fileName = appinfoDict["filename"]
                if "fileVersion" in appinfoDict:
                    fileVersion = appinfoDict["fileversion"]
                if "productName" in appinfoDict:
                    productName = appinfoDict["productname"]
                if "productversion" in appinfoDict:
                    productVersion = appinfoDict["productversion"]
                if "sha256hashdata" in appinfoDict:
                    sha256HashData = appinfoDict["sha256hashdata"]
                # map old column values to new column names
                appinfoTransformationObj = {
                "LastSeenUTC": lastSeenUTC,
                "cid": cid,
                "CompanyName":companyName,
                # convert detectionCount string to int
                "detectionCount":int(float(detectionCount)),
                "event_platform":eventPlatform,
                "FileDescription":fileDescription,
                "FileName":fileName,
                "FileVersion":fileVersion,
                "ProductName": productName,
                "ProductVersion":productVersion  ,
                "SHA256HashData": sha256HashData
                }
                appinfoTransformationObjList.append(appinfoTransformationObj)
            logging.info("transformation completed for file : {}".format("gs://{}/{}".format(bucketName,bucketPath)))
            return [appinfoTransformationObjList]
        except Exception as e:
            logging.error("Failed to transform data with error {}".format(e))
            raise Exception(e)


class ingestDataToBQ(beam.DoFn):
    """ Loads data to  bigquery table from dictionary. 
        Accepts input as <List> of Dictionary Objects
        Loads this data into bigquery table.
    """
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, element,projectId,appinfoBQDatasetId,appinfoBQTableId):
        try:
            from google.cloud import bigquery
            logging.info("Loading data to bigquery table.")
            # Get bigquery client object.
            bq_client = bigquery.Client(project = projectId)
            # Get required parameters
            table_id = "{}.{}.{}".format(projectId,appinfoBQDatasetId,appinfoBQTableId)
            table = bq_client.get_table(table_id)
            bq_schema = table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            # Load data to BQ
            load_job = bq_client.load_table_from_json(element, table, job_config=job_config)  
            # JSON data loading to BigQuery
            load_job.result()
            logging.info("Successfully completed")
        except Exception as e:
            logging.error("Failed to load data in bigquery table with error {}".format(e))
            raise Exception(e)


def run(argv=None):
    global custom_options
    # Get pipeline options parameters
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomParam)
    # Get Google cloud required
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.job_name = jobname
    google_cloud_options.project = projectId
    google_cloud_options.region = region
    google_cloud_options.service_account_email = serviceAccountEmail
    google_cloud_options.staging_location = stagingLocation
    google_cloud_options.temp_location = tempLocation
    #google_cloud_options.template_location='gs://external_api_data/template/'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(StandardOptions).streaming = False
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.setup_file = './setup.py'
    setup_options.save_main_session = True
    
    pipeline_options.view_as(
        WorkerOptions).subnetwork = subnetwork
    p = beam.Pipeline(options=pipeline_options)
    results = (
            p
            | "Create Pipeline" >> beam.Create(["Start"])
            | 'list files' >> beam.ParDo(getBucketFilesList(),custom_options)
            | 'transformation' >> beam.ParDo(transformation(),custom_options,projectId)
            | 'ingest data to bigquery' >> beam.ParDo(ingestDataToBQ(),projectId,appinfoBQDatasetId,appinfoBQTableId)
                )
    res = p.run().wait_until_finish()

if __name__ == '__main__':
    try:
        # read config.json file
        with open('/workspace/config.yaml') as f:
            config = yaml.load(f, Loader=SafeLoader)
            logging.info(config)
    except Exception as err:
        logging.error("Failed to read config file, error is: {}".format(err))
        raise
    try:
        # with open('config.yaml') as f:
        #     config = yaml.load(f, Loader=SafeLoader)
        #     logging.info("Config file readed successfully.")
        projectId = config["COMMON"]["GCP"]["PROJECT_ID"]
        region = config["COMMON"]["GCP"]["REGION"]
        jobname = config["CROWDSTRIKE_ACCOUNT_AND_ASSETS"]["APPINFO_JOBNAME"]
        tempLocation = config["COMMON"]["DATAFLOW"]["TEMP_LOCATION"]
        stagingLocation = config["COMMON"]["DATAFLOW"]["STAGING_LOCATION"]
        subnetwork = config["COMMON"]["DATAFLOW"]["SUBNETWORK"]
        serviceAccountEmail = config["COMMON"]["GCP"]["SERVICE_ACCOUNT_EMAIL"]
        appinfoBQDatasetId = config["COMMON"]["BIGQUERY"]["CROWDSTRIKE"]["DATASET_ID"]
        appinfoBQTableId = config["COMMON"]["BIGQUERY"]["CROWDSTRIKE"]["APPINFO_TABLE_ID"]
        run()
    except KeyError as e:
        logging.error("key not found in yaml config file error is: {}".format(e))
    except Exception as e:
        logging.error("Failed to execute main function with error: {}".format(e))
