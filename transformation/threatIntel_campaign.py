"""
dataflow for camapaign api

Description: Dataflow will read data from gcs bucket and transform data and ingest data to bigquery.

Steps:
1. Make list of camapign file gcs bucket path
2. Read campaign file data
3. Transform data
4.Ingest data to bigquery

"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions, StandardOptions, \
    WorkerOptions

from google.cloud import bigquery
from google.cloud import storage

import yaml, gcsfs, logging, json
from yaml.loader import SafeLoader


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
        """Read campaign file data from GCS bucket
        --input parameter:
           bucketfilepath : gcs bucket file path

        """
        logging.info("reading content of file from bucket file path is:{}".format(bucketfilepath))
        gcsFileSystem = gcsfs.GCSFileSystem(project=projectId)
        # read json file from gcs bucket and path provided in bucketfilepath variable
        with gcsFileSystem.open(bucketfilepath) as f:
            # json.load() takes a file object and returns the json object.
            campaignDataDict = json.load(f)
        logging.info("successfully read content of file")
        # return campaign data json dictionary
        return campaignDataDict
    except Exception as error:
        logging.error("failed to read file data from bucket error is :{}".format(error))
        raise


class getBucketFilesList(beam.DoFn):
    """Fetch campaign file path from bucket
    Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "threatIntel/".

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `threatIntel/`:

       e.g threatIntel/folderName/campaign.json

    """

    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, element):
        try:
            GcsBucketClient = storage.Client()
            # fetch custom input value which contain GCS bucket folder path of threatIntel files
            bucketFolderPath = str(custom_options.gcsBucketPath.get())
            # extract bucket name from input
            bucketName = bucketFolderPath.split("/", 3)[2]
            # extract folder in the bucket that begin with the prefix
            prefix = bucketFolderPath.split("/", 3)[3]
            logging.info("making list of files for bucket : {} prefix : {}".format(bucketName, prefix))
            campaignFileBucketPath = []
            # Note: The call returns a response only when the iterator is consumed.
            for gcsBucketPath in GcsBucketClient.list_blobs(bucketName, prefix=prefix):
                # check gcs path is campaign file path or not
                if (((gcsBucketPath.name).find("campaign")) != -1):
                    campaignFileBucketPath.append(gcsBucketPath.name)
            logging.info("campaign files bucket path is: {}".format(campaignFileBucketPath))
            return campaignFileBucketPath
        except Exception as error:
            logging.error("Failed to get list of gcs files path error is : {}".format(error))
            raise


class transformation(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, campaignFileBucketPath):
        try:
            # fetch custom input value which contain GCS bucket folder path of threatIntel files
            bucketPath = str(custom_options.gcsBucketPath.get())
            bucketName = bucketPath.split("/", 3)[2]
            campaignDict = readBucketFileData("gs://{}/{}".format(bucketName, campaignFileBucketPath))
            if type(campaignDict) is not dict:
                campaignDict = json.load(campaignDict)
            else:
                campaignDict = campaignDict
            data_list = [campaignDict]
            for row in data_list:
                row['campaign_id'] = row.pop('id') if row['id'] else None
                row['campaign_name'] = row.pop('name') if row['name'] else None
                row['campaign_description'] = row.pop('description') if row['description'] else None
                row['campaign_startDate'] = row.pop('startDate') if row['startDate'] else None

                if row['campaignMembers']:
                    for member in row['campaignMembers']:
                        member['threat_id'] = member.pop('id') if member['id'] else None
                        member['threatType'] = member.pop('type') if member['type'] else None
                        member['threat'] = member.pop('threat') if member['threat'] else None
                        member['threatTime'] = member.pop('threatTime') if member['threatTime'] else None
                        member['threatStatus'] = member.pop('threatStatus') if member['threatStatus'] else None
                if row['actors']:
                    for actor in row['actors']:
                        actor['actor_name'] = actor.pop('name') if actor['name'] else None
                        actor['actor_id'] = actor.pop('id') if actor['id'] else None
                if row['malware']:
                    for malw in row['malware']:
                        malw['malware_name'] = malw.pop('name') if malw['name'] else None
                        malw['malware_id'] = malw.pop('id') if malw['id'] else None
                if row['techniques']:
                    for technique in row['techniques']:
                        technique['technique_name'] = technique.pop('name') if technique['name'] else None
                        technique['technique_id'] = technique.pop('id') if technique['id'] else None
                if row['brands']:
                    for brand in row['brands']:
                        brand['brand_name'] = brand.pop('name') if brand['name'] else None
                        brand['brand_id'] = brand.pop('id') if brand['id'] else None
                if row['families']:
                    for family in row['families']:
                        family['family_name'] = family.pop('name') if family['name'] else None
                        family['family_id'] = family.pop('id') if family['id'] else None
            return data_list
        except Exception as error:
            logging.info("failed to read data from bucket error is:{}".format(error))
            raise


class ingestDataToBQ(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, campaignDict):
        try:
            bigqueryClient = bigquery.Client(project=projectId)
            # tableId contain project ID , threatIntel dataset ID and campaign table name
            tableId = '{}.{}.{}'.format(projectId, threatIntelBQDatasetId, campaignBQTableId)
            logging.info("campaign data will ingest to tableID : {} ".format(tableId))
            table = bigqueryClient.get_table(tableId)
            # fetch bigquery schema of campaign table
            bq_schema = table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema
            # To use newline-delimited JSON, set the LoadJobConfig.source_format property to the string NEWLINE_DELIMITED_JSON
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            # If the table already exists, BigQuery appends the data to the table.
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            # pass the job config as the job_config argument to the load_table_from_json() method.
            load_job = bigqueryClient.load_table_from_json([campaignDict], table,
                                                           job_config=job_config)  # load data to BQ
            logging.info('JSON data loading to BigQuery')
            # load ingest to bigquery
            load_job.result()
            logging.info("Successfully data ingested to bigquery")
        except Exception as error:
            logging.error("Failed to ingest data into bigquery:{}".format(error))
            raise


def run(argv=None):
    global custom_options
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomParam)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.job_name = "campaign"
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
        campaignBQTableId = config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["CAMPAIGN_TABLE_ID"]
        machineType = config["COMMON"]["DATAFLOW"]["MACHINE_TYPE"]
        diskSize = config["COMMON"]["DATAFLOW"]["DISK_SIZE"]
        run()
    except KeyError as e:
        logging.error("key not found in yaml config file error is: {}".format(e))
    except Exception as e:
        logging.error("failed to execute pipeline error:{}".format(e))

