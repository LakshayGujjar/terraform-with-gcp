"""
dataflow for forensics api

Description: Dataflow will read data from gcs bucket and transform data and ingest data to bigquery.

Steps:
1. Make list of forensic file gcs bucket path
2. Read forensic file data
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
        """Read forensics file data from GCS bucket
           bucketfilepath : gcs bucket file path"""
        logging.info("reading content of file from bucket file path is:{}".format(bucketfilepath))
        gcsFileSystem = gcsfs.GCSFileSystem(project=projectId)
        # read json file from gcs bucket and path provided in bucketfilepath variable
        with gcsFileSystem.open(bucketfilepath) as f:
            # json.load() takes a file object and returns the json object.
            forensicsDataDict = json.load(f)
        logging.info("successfully read content of file")
        # return forensics data json dictionary
        return forensicsDataDict
    except Exception as error:
        logging.error("failed to read file data from bucket error is :{}".format(error))
        raise


class getBucketFilesList(beam.DoFn):
    """Fetch forensics file path from bucket
    Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "threatIntel/".

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `threatIntel/`:

       e.g threatIntel/folderName/forensics.json

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
            forensicsFileBucketPath=[]
            # Note: The call returns a response only when the iterator is consumed.
            for gcsBucketPath in GcsBucketClient.list_blobs(bucketName , prefix=prefix):
                #check gcs path is forensics file path or not
                if(((gcsBucketPath.name).find("forensics"))!=-1):
                    forensicsFileBucketPath.append(gcsBucketPath.name)
            logging.info("thread files bucket path is: {}".format(forensicsFileBucketPath))
            return forensicsFileBucketPath
        except Exception as error:
            logging.error("Failed to get list of gcs files path error is : {}".format(error))
            raise

class transformation(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, forensicsFileBucketPath):
        try:
            #fetch custom input value which contain GCS bucket folder path of threatIntel files
            bucketPath = str(custom_options.gcsBucketPath.get())
            bucketName = bucketPath.split("/", 3)[2]
            forensicsDict = readBucketFileData("gs://{}/{}".format(bucketName, forensicsFileBucketPath))
            if type(forensicsDict) is not dict:  #check if data in dictionary
                forensicsDict = json.load(forensicsDict)
            else:
                forensicsDict = forensicsDict
            logging.info("Data read is successfull. Processing for transformations on data.")
            final_data={}   #contains final forensicsDict data with transformation changes
            index=0
            if "generated" in forensicsDict.keys():  #check and assign if key exists in dic
                final_data['report_time'] = forensicsDict["generated"]
            for report in forensicsDict["reports"]:
                if "name" in report.keys():     #check and assign if key exists in dic
                    final_data['report_name'] = report["name"]
                if "scope" in report.keys():    #check and assign if key exists in dic
                    final_data['report_scope'] = report["scope"]
                if "threatStatus" in report.keys():     #check and assign if key exists in dic
                    final_data['threat_status'] = report["threatStatus"]
                if "id" in report.keys():       #check and assign if key exists in dic
                    final_data['threat_id'] = report["id"]
                final_data['forensics'] = []
                if report['forensics']:
                    for forensic in report['forensics']:
                        forensics_dic={}
                        if "type" in forensic.keys():   #check and assign if key exists in dic
                            forensics_dic['evidence_type'] = forensic["type"]
                        if "display" in forensic.keys():    #check and assign if key exists in dic
                            forensics_dic['display'] = forensic["display"]
                        if "malicious" in forensic.keys():  #check and assign if key exists in dic
                            forensics_dic['malicious'] = forensic["malicious"]
                        if "time" in forensic.keys():   #check and assign if key exists in dic
                            forensics_dic['time'] = forensic["time"]
                        if "note" in forensic.keys():   #check and assign if key exists in dic
                            forensics_dic['note'] = forensic["note"]
                        if "engine" in forensic.keys():     #check and assign if key exists in dic
                            forensics_dic['engine'] = forensic["engine"]
                        if "platforms" in forensic.keys():      #check and assign if key exists in dic
                            forensics_dic['platforms'] = forensic['platforms']
                        forensics_dic["what"]={}
                        if "type" in forensic["what"].keys():   #check and assign if key exists in dic
                            forensics_dic['what']['network_protocol'] = forensic['what']["type"]
                        if "ip" in forensic["what"].keys():     #check and assign if key exists in dic
                            if "ips" in forensic["what"].keys():    #check and assign if key exists in dic
                                forensics_dic['what']['dest_ip'] = forensic['what']["ips"].append(forensic['what']["ip"])
                            else:
                                forensics_dic['what']['dest_ip'] = [forensic['what']["ip"]]
                        elif "ips" in forensic["what"].keys():    #check and assign if key exists in dic
                            forensics_dic['what']['dest_ip'] = forensic['what']["ips"]
                        if "cnames" in forensic["what"].keys():     #check and assign if key exists in dic
                            forensics_dic['what']['cnames'] = forensic['what']["cnames"]
                        if "port" in forensic["what"].keys():       #check and assign if key exists in dic
                            forensics_dic['what']['dest_port'] = forensic['what']["port"]
                        if "action" in forensic["what"].keys():     #check and assign if key exists in dic
                            forensics_dic['what']['action'] = forensic['what']["action"]
                        if "name" in forensic["what"].keys():   #check and assign if key exists in dic
                            forensics_dic['what']['name'] = forensic['what']["name"]
                        if "url" in forensic["what"].keys():    #check and assign if key exists in dic
                            forensics_dic["what"]["url"] = forensic["what"]["url"]
                        if "blacklisted" in forensic["what"].keys():    #check and assign if key exists in dic
                            if forensic['what']["blacklisted"] == 0:
                                forensics_dic['what']['blacklisted'] = False
                            elif forensic['what']["blacklisted"] > 0:
                                forensics_dic['what']['blacklisted'] = True
                            else:
                                forensics_dic['what']['blacklisted'] = forensic['what']["blacklisted"]
                        if "path" in forensic["what"].keys():   #check and assign if key exists in dic
                            forensics_dic['what']['path'] = forensic['what']["path"]
                        if "rule" in forensic["what"].keys():   #check and assign if key exists in dic
                            forensics_dic['what']['rule'] = forensic['what']["rule"]
                        if "size" in forensic["what"].keys():   #check and assign if key exists in dic
                            forensics_dic['what']['size'] = forensic['what']["size"]
                        if "sha256" in forensic["what"].keys():     #check and assign if key exists in dic
                            forensics_dic['what']['sha256'] = forensic['what']["sha256"]
                        if "host" in forensic["what"].keys():       #check and assign if key exists in dic
                            forensics_dic['what']['host'] = forensic['what']["host"]
                        final_data['forensics'].append(forensics_dic)
            logging.info("Processing data")
            return [final_data]
        except Exception as error:
            logging.debug("Failed on transformation function with error:{}".format(error))
            raise Exception("Failed transformation function with error:{}".format(error))


class ingestDataToBQ(beam.DoFn):
    def __init__(self):
        # super(WordExtractingDoFn, self).__init__()
        beam.DoFn.__init__(self)

    def process(self, forensicsDict):
        try:
            bigqueryClient = bigquery.Client(project=projectId)
            #tableId contain project ID , threatIntel dataset ID and forensics table name
            tableId = '{}.{}.{}'.format(projectId,threatIntelBQDatasetId,forensicsBQTableId)
            logging.info("forensics data will ingest to tableID : {} ".format(tableId))
            table = bigqueryClient.get_table(tableId)
            #fetch bigquery schema of forensics table
            bq_schema = table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema
            #To use newline-delimited JSON, set the LoadJobConfig.source_format property to the string NEWLINE_DELIMITED_JSON
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            #If the table already exists, BigQuery appends the data to the table.
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            #pass the job config as the job_config argument to the load_table_from_json() method.
            load_job = bigqueryClient.load_table_from_json([forensicsDict], table, job_config=job_config) #load data to BQ
            logging.info('JSON data loading to BigQuery')
            #load ingest to bigquery
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
    google_cloud_options.job_name = "forensics"
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
        forensicsBQTableId = config["COMMON"]["BIGQUERY"]["PROOFPOINT"]["FORENSICS_TABLE_ID"]
        machineType = config["COMMON"]["DATAFLOW"]["MACHINE_TYPE"]
        diskSize = config["COMMON"]["DATAFLOW"]["DISK_SIZE"]
        run()
    except KeyError as e:
        logging.error("key not found in yaml config file error is: {}".format(e))
    except Exception as e:
        logging.error("failed to execute pipeline error:{}".format(e))




