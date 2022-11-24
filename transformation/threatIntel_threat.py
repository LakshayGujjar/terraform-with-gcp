"""
dataflow for forensics api

Description: Dataflow will read data from gcs bucket and transform data and ingest data to bigquery.

Steps:
1. Make list of camapign file gcs bucket path
2. Read campaign file data
3. Transform data
4.Ingest data to bigquery

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
from datetime import datetime,timezone

class CustomParam(PipelineOptions):
    """You can add your own custom parameter options in addition to the standard PipelineOptions.
        we have added gcsBucketPath as custom parameter.
        gcsBucketPath : gcs bucket folder path which contain files"""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--gcsBucketPath', dest='gcsBucketPath', required=False,
                                           help='gcs bucket folder path which contain files')


def readBucketFileData(bucketfilepath):
    try:
        """Read threat file data from GCS bucket
           bucketfilepath : gcs bucket file path"""
        logging.info("reading content of file from bucket file path is:{}".format(bucketfilepath))
        gcsFileSystem = gcsfs.GCSFileSystem(project=projectId)
        # read json file from gcs bucket and path provided in bucketfilepath variable
        with gcsFileSystem.open(bucketfilepath) as f:
            # json.load() takes a file object and returns the json object.
            threatDataDict = json.load(f)
        logging.info("successfully read content of file")
        # return threat data json dictionary
        return threatDataDict
    except Exception as error:
        logging.error("failed to read file data from bucket error is :{}".format(error))
        raise
#threatID

class getBucketFilesList(beam.DoFn):
    """Fetch threat file path from bucket
    Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "threatIntel/".

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `threatIntel/`:

       e.g threatIntel/folderName/threat.json

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
            threatFileBucketPath=[]
            # Note: The call returns a response only when the iterator is consumed.
            for gcsBucketPath in GcsBucketClient.list_blobs(bucketName , prefix=prefix):
                #check gcs path is threat file path or not
                if(((gcsBucketPath.name).find("threat_"))!=-1):
                    threatFileBucketPath.append(gcsBucketPath.name)
            logging.info("threat files bucket path is: {}".format(threatFileBucketPath))
            return threatFileBucketPath
        except Exception as error:
            logging.error("Failed to get list of gcs files path error is : {}".format(error))
            raise

class transformation(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, threatFileBucketPath):
        try:
            #fetch custom input value which contain GCS bucket folder path of threatIntel files
            bucketPath = str(custom_options.gcsBucketPath.get())
            bucketName = bucketPath.split("/", 3)[2]
            threatDict = readBucketFileData("gs://{}/{}".format(bucketName, threatFileBucketPath))
            if 'id' in threatDict.keys():   #check and assign if key exists in dic
                threatDict['threat_id'] = threatDict.pop('id')
            if 'name' in threatDict.keys(): #check and assign if key exists in dic
                threatDict['threat_name'] = threatDict.pop('name')
            if 'type' in threatDict.keys(): #check and assign if key exists in dic
                threatDict['threat_type'] = threatDict.pop('type')
            if 'category' in threatDict.keys(): #check and assign if key exists in dic
                threatDict['threat_category'] = threatDict.pop('category')
            if 'status' in threatDict.keys():   #check and assign if key exists in dic
                threatDict['threat_status'] = threatDict.pop('status')
            threatDict['updatedAt'] = str(datetime.now(timezone.utc))
            if 'actors' in threatDict.keys():   #check and assign if key exists in dic
                for actor in threatDict['actors']:
                    if 'name' in actor.keys():  #check and assign if key exists in dic
                        actor['actor_name'] = actor.pop('name')
                    if 'id' in actor.keys():    #check and assign if key exists in dic
                        actor['actor_id'] = actor.pop('id')
            if 'malware' in threatDict.keys():  #check and assign if key exists in dic
                for malw in threatDict['malware']:
                    if 'name' in malw.keys():   #check and assign if key exists in dic
                        malw['malware_name'] = malw.pop('name')
                    if 'id' in malw.keys(): #check and assign if key exists in dic
                        malw['malware_id'] = malw.pop('id')
            if 'techniques' in threatDict.keys():   #check and assign if key exists in dic
                for technique in threatDict['techniques']:
                    if 'name' in technique.keys():  #check and assign if key exists in dic
                        technique['technique_name'] = technique.pop('name')
                    if 'id' in technique.keys():    #check and assign if key exists in dic
                        technique['technique_id'] = technique.pop('id')
            if 'brands' in threatDict.keys():   #check and assign if key exists in dic
                for brand in threatDict['brands']:
                    if 'name' in brand.keys():  #check and assign if key exists in dic
                        brand['brand_name'] = brand.pop('name')
                    if 'id' in brand.keys():    #check and assign if key exists in dic
                        brand['brand_id'] = brand.pop('id')
            if 'families' in threatDict.keys():
                for family in threatDict['families']:
                    if 'name' in family.keys(): #check and assign if key exists in dic
                        family['family_name'] = family.pop('name')
                    if 'id' in family.keys():   #check and assign if key exists in dic
                        family['family_id'] = family.pop('id')

            return [threatDict]
        except Exception as error:
            logging.info("failed to read data from bucket error is:{}".format(error))
            raise


class ingestDataToBQ(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, threatDict):
        try:
            bigqueryClient = bigquery.Client(project=projectId)
            #tableId contain project ID , threatIntel dataset ID and threat table name
            tableId = '{}.{}.{}'.format(projectId,threatIntelBQDatasetId,threatBQTableId)
            logging.info("threat data will ingest to tableID : {} ".format(tableId))
            table = bigqueryClient.get_table(tableId)
            #fetch bigquery schema of threat table
            bq_schema = table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema
            #To use newline-delimited JSON, set the LoadJobConfig.source_format property to the string NEWLINE_DELIMITED_JSON
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            #If the table already exists, BigQuery appends the data to the table.
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            #pass the job config as the job_config argument to the load_table_from_json() method.
            load_job = bigqueryClient.load_table_from_json([threatDict], table, job_config=job_config) #load data to BQ
            logging.info('JSON data loading to BigQuery')
            #load ingest to bigquery
            load_job.result()
            logging.info("Successfully data ingested to bigquery")
        except Exception as error:
            logging.error("Failed to ingest data into bigqury:{}".format(error))
            raise

def run(argv=None):
    global custom_options
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomParam)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.job_name = "threat"
    google_cloud_options.project = projectId
    google_cloud_options.region = region
    google_cloud_options.service_account_email = serviceAccountEmail
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
            | "Create Pipeline" >> beam.Create(["Start"])
            | 'fetch data from bucket' >> beam.ParDo(getBucketFilesList())
            | 'transformation' >> beam.ParDo(transformation())
            | 'ingest to bigquery' >> beam.ParDo(ingestDataToBQ())
    )

    res = p.run().wait_until_finish()


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
        region = config["COMMON"]["GCP"]["REGION"]
        tempLocation = config["COMMON"]["DATAFLOW"]["TEMP_LOCATION"]
        stagingLocation = config["COMMON"]["DATAFLOW"]["STAGING_LOCATION"]
        subnetwork = config["COMMON"]["DATAFLOW"]["SUBNETWORK"]
        serviceAccountEmail = config["COMMON"]["GCP"]["SERVICE_ACCOUNT_EMAIL"]
        threatIntelBQDatasetId = config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["DATASET_ID"]
        threatBQTableId = config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["THREAT_TABLE_ID"]
        machineType = config["COMMON"]["DATAFLOW"]["MACHINE_TYPE"]
        diskSize = config["COMMON"]["DATAFLOW"]["DISK_SIZE"]
        run()
    except KeyError as e:
        logging.error("key not found in yaml config file error is: {}".format(e))
    except Exception as e:
        logging.error("failed to execute pipeline error:{}".format(e))