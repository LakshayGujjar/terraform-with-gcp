"""
dataflow for siem clicks api

Description: Dataflow will read data from gcs bucket and transform data and ingest data to bigquery.

Steps:
1. Make list of siem file gcs bucket path
2. Read siem file data
3. Transform data
4. Ingest data to bigquery

"""
#Importing dependancies
from apache_beam.options.pipeline_options import WorkerOptions
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import bigquery
import yaml
from yaml.loader import SafeLoader
import gcsfs
import json
import logging
from google.cloud import storage


class CustomParam(PipelineOptions):
    """You can add your own custom parameter options in addition to the standard PipelineOptions.
    we have added gcsBucketPath as custom parameter.
    gcsBucketPath : gcs bucket folder path which contain files"""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--gcsBucketPath', dest='gcsBucketPath', required=False, help='gcs bucket folder path')


def readBucketFileData(bucketfilepath):
    """Read siem file data from GCS bucket
        bucketfilepath : gcs bucket file path"""
    try:
        logging.info("reading content of file from bucket file path is:{}".format(bucketfilepath))
        gcsFileSystem = gcsfs.GCSFileSystem(project=projectId)
        # read json file from gcs bucket and path provided in bucketfilepath variable
        with gcsFileSystem.open(bucketfilepath) as f:
            # json.load() takes a file object and returns the json object.
            siemDataDict = json.load(f)
        logging.info("successfully read content of file")
        # return siem data json dictionary
        return siemDataDict
    except Exception as error:
        logging.error("failed to read file data from bucket error is :{}".format(error))
        raise


class getBucketFilesList(beam.DoFn):
    """Fetch siem file path from bucket
    Lists all the blobs(files) in the bucket that begin with the prefix(threatIntel).

    This can be used to list all blobs in a "folder", e.g. "threatIntel/".

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `threatIntel/`:

       e.g threatIntel/folderName/siem.json

    """
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, element):
        try:
            GcsBucketClient = storage.Client()
            #fetch custom input value which contain GCS bucket folder path of threatIntel files
            bucketFolderPath = str(custom_options.gcsBucketPath.get())
            #extract bucket name from input
            bucketName=bucketFolderPath.split("/",3)[2]
            #extract folder in the bucket that begin with the prefix
            prefix=bucketFolderPath.split("/",3)[3]
            logging.info("making list of files for bucket : {} prefix : {}".format(bucketName,prefix))
            siemBucketPath=[]
            # Note: The call returns a response only when the iterator is consumed.
            for gcsBucketPath in GcsBucketClient.list_blobs(bucketName , prefix=prefix):
                #check gcs path is siem file path or not
                if(((gcsBucketPath.name).find("siem"))!=-1):
                    siemBucketPath.append(gcsBucketPath.name)
            logging.info("siem file bucket path is: {}".format(siemBucketPath))
            return siemBucketPath
        except Exception as error:
            logging.error("Failed to get list of gcs files path error is : {}".format(error))
            raise

class transformation(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, forensicsFileBucketPath):
        try:
            bucketPath = str(custom_options.gcsBucketPath.get())
            bucketName=bucketPath.split("/",3)[2]
            dictionary = readBucketFileData("gs://{}/{}".format(bucketName,forensicsFileBucketPath))
            dictlist=[]
            for clicksBlocked in dictionary["clicksBlocked"]:
                if(len(dictionary["clicksBlocked"])!=0):
                    data={"eventType":"clicksBlocked","queryEndTime":dictionary["queryEndTime"] }
                    clicksBlocked['campaignID'] = clicksBlocked.pop('campaignId')
                    clicksBlocked['senderEmail'] = clicksBlocked.pop('sender')
                    clicksBlocked['recipientEmail'] = clicksBlocked.pop('recipient')
                    clicksBlocked['clickID'] = clicksBlocked.pop('id')
                    clicksBlocked.update(data)
                    dictlist.append(clicksBlocked)
            for clicksPermitted in dictionary["clicksPermitted"]:
                if(len(dictionary["clicksPermitted"])!=0):
                    data={"eventType":"clicksPermitted","queryEndTime":dictionary["queryEndTime"] }
                    clicksPermitted['campaignID'] = clicksPermitted.pop('campaignId')
                    clicksPermitted['senderEmail'] = clicksPermitted.pop('sender')
                    clicksPermitted['recipientEmail'] = clicksPermitted.pop('recipient')
                    clicksPermitted['clickID'] = clicksPermitted.pop('id')
                    clicksPermitted.update(data)
                    dictlist.append(clicksPermitted)
                    clicksPermitted.update(data)
                    dictlist.append(clicksPermitted)

            return [dictlist]
        except Exception as e:
            logging.info("failed to fetch list{}".format(e))
            raise

class ingestDataToBQ(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, clickEvnetsDict):
        try:
            bigqueryClient = bigquery.Client(project=projectId)
            #tableId contain project ID , threatIntel dataset ID and siem_clicksEvent table name
            tableId = '{}.{}.{}'.format(projectId,threatIntelDatasetId,siemClickEventTableId)
            logging.info("clickEvents data will ingest to tableID : {} ".format(tableId))
            table = bigqueryClient.get_table(tableId)
            #fetch bigquery schema of siem_clicksEvents table
            bq_schema = table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema
            #To use newline-delimited JSON, set the LoadJobConfig.source_format property to the string NEWLINE_DELIMITED_JSON
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            #If the table already exists, BigQuery appends the data to the table.
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            #pass the job config as the job_config argument to the load_table_from_json() method.
            load_job = bigqueryClient.load_table_from_json(clickEvnetsDict, table, job_config=job_config) #load data to BQ
            logging.info('JSON data loading to BigQuery')
            #load ingest to bigquery
            load_job.result()
            logging.info("Successfully completed")
        except Exception as error:
            logging.error("Failed to ingest data into bigqury:{}".format(error))
            raise


def run(argv=None):
    global custom_options
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomParam)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.job_name = "siem-click"
    google_cloud_options.project = projectId
    google_cloud_options.region = region
    google_cloud_options.service_account_email=serviceAccountEmail
    google_cloud_options.staging_location = stagingLocation
    google_cloud_options.temp_location = tempLocation
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(StandardOptions).streaming = False
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.setup_file = './setup.py'
    pipeline_options.view_as(WorkerOptions).machine_type = machineType
    pipeline_options.view_as(WorkerOptions).disk_size_gb = diskSize
    setup_options.save_main_session = True
    pipeline_options.view_as(
    WorkerOptions).subnetwork = subnetwork
    p = beam.Pipeline(options=pipeline_options)
    results = (
        p
        |"Create Pipeline" >> beam.Create(["Start"])
        | 'fetch data from bucket' >> beam.ParDo(getBucketFilesList())
        | 'transformation' >> beam.ParDo(transformation())
        | 'ingest to bigquery' >> beam.ParDo(ingestDataToBQ())
        )

    res=p.run().wait_until_finish()


if __name__ == '__main__':
    try:
        # read config.json file
        with open('config.yaml') as f:
            config = yaml.load(f, Loader=SafeLoader)
            logging.info(config)
    except Exception as err:
        logging.error("Failed to read config file, error is: {}".format(err))
        raise
    try:
        projectId = config["COMMON"]["GCP"]["PROJECT_ID"]
        region=config["COMMON"]["GCP"]["REGION"]
        tempLocation=config["COMMON"]["DATAFLOW"]["TEMP_LOCATION"]
        stagingLocation=config["COMMON"]["DATAFLOW"]["STAGING_LOCATION"]
        subnetwork=config["COMMON"]["DATAFLOW"]["SUBNETWORK"]
        serviceAccountEmail=config["COMMON"]["GCP"]["SERVICE_ACCOUNT_EMAIL"]
        threatIntelDatasetId=config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["DATASET_ID"]
        siemClickEventTableId=config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["SIEM_CLICK_EVENTS_TABLE_ID"]
        machineType = config["COMMON"]["DATAFLOW"]["MACHINE_TYPE"]
        diskSize = config["COMMON"]["DATAFLOW"]["DISK_SIZE"]
        run()
    except KeyError as e:
        logging.error("key not found in yaml config file error is: {}".format(e))
    except Exception as e:
        logging.error("failed to execute svs pipeline error:{}".format(e))





