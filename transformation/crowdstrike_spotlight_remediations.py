#  Copyright 2022 Google LLC. This software is provided as is,
#  without warranty or representation for any use or purpose.
#  Your use of it is subject to your agreement with Google.
"""
Full Scorecard Dataflow batch pipeline:
It reads JSON files from GCS, transforms the json data and
writes the results to BigQuery.
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions, StandardOptions

from google.cloud import bigquery, storage
from google.cloud import exceptions

import yaml
from yaml.loader import SafeLoader
import pandas as pd
import numpy as np
import json
import logging
import warnings
from datetime import datetime, date, timedelta

warnings.filterwarnings('ignore')

'''START'''


class CustomParam(PipelineOptions):
    """You can add your own custom parameter options in addition to the standard PipelineOptions.
   we have added gcsBucketPath as custom parameter.
   gcsBucketPath : gcs bucket folder path which contain files"""

    @classmethod
    def _add_argparse_args(cls, parser):
        # custom input parameter declaration
        parser.add_value_provider_argument('--gcsBucketPath', dest='gcsBucketPath', required=False,
                                           help='Json file path')


class Accessing_files_on_GCS(beam.DoFn):
    """Accessing JSON files of Scorecard API
            args: gcs bucket
            return: list of json paths"""

    def setup(self):
        self.storage_client = storage.Client()

    def process(self, element):
        try:
            gcsBucketPath = str(custom_options.gcsBucketPath.get())
            logging.info(gcsBucketPath)
            BUCKET_NAME = gcsBucketPath.split("/", 3)[2]
            logging.info(BUCKET_NAME)
            PREFIX = gcsBucketPath.split("/", 3)[3]
            logging.info(PREFIX)
            '''selecting the json file paths from the bucket'''
            blobs = self.storage_client.list_blobs(BUCKET_NAME, prefix=PREFIX)
            json_filepaths = []
            for blob in blobs:
                json_filepaths.append(f"{blob.name}")

        except Exception as error:
            logging.error("Failed to access the JSON file from GCS path,ERROR: {}".format(error))
            

        return json_filepaths


class ReadFileContent(beam.DoFn):
    """Accessing JSON files of Scorecard API
                args: json paths and gcs bucket
                return: JSON object"""

    def setup(self):
        self.storage_client = storage.Client()

    def process(self, json_paths):
        try:
            gcsBucketPath = str(custom_options.gcsBucketPath.get())
            BUCKET_NAME = gcsBucketPath.split("/", 3)[2]
            bucket = self.storage_client.get_bucket(BUCKET_NAME)
            blob = bucket.blob(json_paths)
            '''loading API data to a variable'''
            input_data = json.loads(blob.download_as_string(client=None))

        except (AttributeError, exceptions.NotFound) as err:
            logging.error(f'failed to retrieve Blob with name %r from Google Cloud Storage Bucket', {BUCKET_NAME},
                          {err})
            

        except Exception as error:
            logging.error("Failed to read the data from JSON path,ERROR: {}".format(error))

        yield input_data


class Transformation(beam.DoFn):
    """Transforming the input data, this transformation include deleting unnecessary columns, renaming columns, flattening nested columns
            args:json object
            return: transformed data in pandas"""

    def __init__(self):
        beam.DoFn.__init__(self)

    def process(self, element):
        try:
            '''load input data into pandas dataframe'''
            print("transformassion starting")
            data_list = [element]
            df = pd.DataFrame(data_list)
            df = df.drop(['meta'], axis=1) # delete metadata cols
            if df['resources'][0] != []:
               if 'resources' in df.columns:
                    df = pd.concat([df.explode('resources').drop(['resources'], axis=1),
                    df.explode('resources')['resources'].apply(pd.Series)],axis=1)
                
            else:
                logging.debug("resources key is empty")
            df = df.applymap(str)
            
        except Exception as error:
            logging.error("Failed to transform the input,ERROR: {}".format(error))

        return [df]


class Ingest_data_to_bq(beam.DoFn):
    """Ingesting the transformed data into BigQuery
                args:list of pandas dataframe
                return: None"""

    def __init__(self):
        beam.DoFn.__init__(self)

    def process(self, df):
        try:
            logging.debug("ingest")

            bq_client = bigquery.Client(project=PROJECT_ID)

            bqdf = pd.DataFrame(df)

            table_id = '{0}.{1}.{2}'.format(PROJECT_ID, DATASET_ID, TABLE_ID)

            bq_input = bqdf.to_dict('records')

            table = bq_client.get_table(table_id)
            bq_schema = table.schema
            job_config = bigquery.job.LoadJobConfig()
            job_config.schema = bq_schema

            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.schema_update_options = 'ALLOW_FIELD_ADDITION'
            job = bq_client.load_table_from_json(bq_input, table, job_config=job_config)

            job.result()
            logging.debug('Ingested data to Bigquery')

        except Exception as err:
            logging.error("Failed to ingest into BigQuery,ERROR:{}".format(err))
            


def run(argv=None):
    global custom_options
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomParam)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.job_name = "spotlight-remediation"
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.region = REGION
    google_cloud_options.service_account_email = SERVICE_ACCOUNT_EMAIL
    google_cloud_options.staging_location = DATAFLOW_STAGING
    google_cloud_options.temp_location = DATAFLOW_TEMP
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(StandardOptions).streaming = False
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.setup_file = './setup.py'
    setup_options.save_main_session = True
    pipeline_options.view_as(
        WorkerOptions).subnetwork = SUBNETWORK

    p = beam.Pipeline(options=pipeline_options)

    results = (
            p
            | "Create Pipeline" >> beam.Create(["Start"])
            | "Access JSON files on GCS" >> beam.ParDo(Accessing_files_on_GCS())
            | "Read each file content" >> beam.ParDo(ReadFileContent())
            | "Transformations" >> beam.ParDo(Transformation())
            | "Write to BigQuery" >> beam.ParDo(Ingest_data_to_bq())
    )

    res = p.run().wait_until_finish()


'''Program starts here'''
if __name__ == '__main__':
    logging.info('Security Scorecard dataflow pipeline execution started')
    warnings.filterwarnings('ignore')
    logging.debug("Reading variables from YAML configuration file")

    try:
        # read config.json file
        with open('config1.yaml') as f:
            config = yaml.load(f, Loader=SafeLoader)
            logging.info(config)
    except Exception as err:
        logging.error("Failed to read config file, error is: {}".format(err))
        raise

    try:
        # variable extraction
        BUCKET_NAME = config['CROWDSTRIKE_SPOTLIGHT']['OUTPUT_BUCKET']
        SERVICE_ACCOUNT_EMAIL = config["COMMON"]["GCP"]["SERVICE_ACCOUNT_EMAIL"]
        PROJECT_ID = config["COMMON"]['GCP']['PROJECT_ID']
        REGION = config["COMMON"]['GCP']['REGION']
        DATAFLOW_STAGING = config["COMMON"]['DATAFLOW']['STAGING_LOCATION']
        DATAFLOW_TEMP = config["COMMON"]['DATAFLOW']['TEMP_LOCATION']
        SUBNETWORK = config["COMMON"]['DATAFLOW']['SUBNETWORK']
        DATASET_ID = config["COMMON"]['BIGQUERY']['SPOTLIGHT']['DATASET_ID']
        TABLE_ID = config["COMMON"]['BIGQUERY']['SPOTLIGHT']['REMEDIATIONS_TABLE_ID']

    except (ValueError, KeyError) as e:
        logging.error("failed to read config variable, %s", e)
        raise KeyError(f"failed to read config variable, {e}")

    run()
    logging.info(f"crowdstrike dataflow execution completed successfully on {date.today()}")