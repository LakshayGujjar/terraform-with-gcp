"""
dataflow for vap api

Description: Dataflow will read data from gcs bucket and transform data and ingest data to bigquery.

Steps:
1. Make list of camapign file gcs bucket path
2. Read campaign file data
3. Transform data
4.Ingest data to bigquery

"""
# Importing dependancies
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
from datetime import datetime, timezone
from google.cloud import storage


class CustomParam(PipelineOptions):
    """You can add your own custom parameter options in addition to the standard PipelineOptions.
   we have added gcsBucketPath as custom parameter.
   gcsBucketPath : gcs bucket folder path which contain files"""

    @classmethod
    def _add_argparse_args(cls, parser):
        # custom input parameter declaration
        parser.add_value_provider_argument('--gcsBucketPath', dest='gcsBucketPath', required=False,
                                           help='Json file path')


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


class transformation(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, element):
        try:
            vapBucketFilePath = str(custom_options.gcsBucketPath.get())
            vapJsonDict = readBucketFileData(vapBucketFilePath)
            transformationJsonObjectList = []
            emailFound = ""
            guid = None
            customerUserId = None
            emailAddressses = None
            name = None
            department = None
            location = None
            vip = None
            title = None
            attackIndex = None
            logging.info("transformation starting for vap api")
            # if want to add new column to data you can add in transformationJsonObject dictionary
            for usersObject in vapJsonDict["users"]:
                identityObjKeys = usersObject["identity"].keys()
                # check if guid key present or not
                if "guid" in identityObjKeys:
                    guid = usersObject["identity"]["guid"]
                if "customerUserId" in identityObjKeys:
                    customerUserId = usersObject["identity"]["customerUserId"]
                if "emails" in identityObjKeys:
                    emailAddressses = usersObject["identity"]["emails"][0]
                    agencyDomain = (usersObject["identity"]["emails"][0]).split("@", 1)[1]
                if "name" in identityObjKeys:
                    name = usersObject["identity"]["name"]
                if "department" in identityObjKeys:
                    department = usersObject["identity"]["department"]
                if "location" in identityObjKeys:
                    location = usersObject["identity"]["location"]
                if "title" in identityObjKeys:
                    title = usersObject["identity"]["title"]
                if "vip" in identityObjKeys:
                    vip = usersObject["identity"]["vip"]
                threatStatisticsKeys = usersObject["threatStatistics"].keys()
                if "attackIndex" in threatStatisticsKeys:
                    attackIndex = usersObject["threatStatistics"]["attackIndex"]
                if "families" in threatStatisticsKeys:
                    # transforming users dictionaries one by one
                    for threatStatisticsDict in usersObject["threatStatistics"]["families"]:
                        try:
                            # reading threatStatistics dictionary and combining name and score values to emailfound for each users
                            emailFound = "{} {} {} ,".format(emailFound, threatStatisticsDict["name"],
                                                             threatStatisticsDict["score"])
                        except Exception as error:
                            logging.error("Failed to transform emailFound error is :{}".format(error))
                # extracting agencyDomain value from emails
                transformationJsonObject = {
                    "Timestamp": str(datetime.now(timezone.utc)),  # adding UTC timestamp
                    "Guid": guid,
                    "CustomerUserId": customerUserId,
                    "EmailAddressses": emailAddressses,
                    "AgencyDomain": agencyDomain,
                    "Name": name,
                    "Agency": "",
                    "Department": department,
                    "Location": location,
                    "Title": title,
                    "VIP": vip,
                    "AttackIndex": attackIndex,
                    "Emails_Found": emailFound[:-1]
                    # [:-1] will remove extra comma which appended to emailFound at last
                }
                json.dumps(transformationJsonObject, indent=4, sort_keys=True, default=str)
                # making list of all transformation json dictionaries
                transformationJsonObjectList.append(transformationJsonObject)
            logging.info("Transformation completed for vap api")
            # passing transformationJsonObjectList to ingest_to_bq function
            return [transformationJsonObjectList]
        except Exception as error:
            logging.error("Transformation failed error is : {}".format(error))
            raise Exception("Transformation failed error is : {}".format(error))


class ingestDataToBQ(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, transformationJsonObjectList):
        try:
            # print(transformationJsonObjectList)
            bq_client = bigquery.Client(project=projectId)
            table_id = '{}.{}.{}'.format(projectId, vapDatasetId, vapTableId)
            table = bq_client.get_table(table_id)
            bq_schema = table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            load_job = bq_client.load_table_from_json(transformationJsonObjectList, table,
                                                      job_config=job_config)  # load data to BQ
            logging.info('JSON data loading to BigQuery')
            # ingesting data into bigquery
            load_job.result()
            logging.info("Successfully data ingested to bigquery")
        except Exception as error:
            logging.error("Failed to ingest data into bigquery errror is :{}".format(error))


def run(argv=None):
    global custom_options
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomParam)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.job_name = "vap"
    google_cloud_options.project = projectId
    google_cloud_options.region = region
    google_cloud_options.service_account_email = serviceAccountEmail
    google_cloud_options.staging_location = stagingLocation
    google_cloud_options.temp_location = tempLocation
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
            | 'transformation' >> beam.ParDo(transformation())
            | 'ingest data to BQ' >> beam.ParDo(ingestDataToBQ())
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
        vapDatasetId = config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["DATASET_ID"]
        vapTableId = config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["VAP_TABLE_ID"]
        machineType=config["COMMON"]["DATAFLOW"]["MACHINE_TYPE"]
        diskSize=config["COMMON"]["DATAFLOW"]["DISK_SIZE"]
        run()
    except Exception as e:
        logging.error("failed to execute svs pipeline error:{}".format(e))








