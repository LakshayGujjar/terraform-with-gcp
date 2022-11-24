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
            BUCKET_NAME = gcsBucketPath.split("/", 3)[2]
            PREFIX = gcsBucketPath.split("/", 3)[3]
            '''selecting the json file paths from the bucket'''
            blobs = self.storage_client.list_blobs(BUCKET_NAME, prefix=PREFIX)
            json_filepaths = []
            for blob in blobs:
                json_filepaths.append(f"{blob.name}")

        except Exception as error:
            logging.error("Failed to access the JSON file from GCS path,ERROR: {}".format(error))
            raise

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
            raise

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
            df = pd.DataFrame(element)
            df3 = pd.DataFrame()
            '''delete metadata cols'''
            df = df.drop(['0', '1', '2', '3', 'reference', 'attribute'], axis=1)
            df = df.iloc[1:, :]  # omitting first row

            def categorise(row):
                # creating alias column for entities
                if row['_t'] == 0.0:
                    return 'score'
                elif row['_t'] == 1.0:
                    return 'factor'
                elif row['_t'] == 2.0:
                    return 'issue'
                elif row['_t'] == 3.0:
                    return 'finding'
                return 'none'

            df['attribute'] = df.apply(lambda row: categorise(row), axis=1)

            df2 = df[['score', 'grade', 'grade_url', 'name']][
                df['attribute'] == 'factor']  # seperated score key for score and factor
            df2.rename(columns=(
                {'score': 'factor_score', 'grade': 'factor_grade', 'grade_url': 'factor_grade_url',
                 'name': 'factor_name'}),
                inplace=True)

            if {'issue_id', 'count'}.issubset(df.columns):
                df3 = df[['issue_id', 'domain', 'count']][
                    df['attribute'] == 'finding']  # seperate count key for issue and finding

            df3.rename(columns=({'issue_id': 'finding_issue_id', 'domain': 'finding_domain', 'count': 'finding_count'}),
                       inplace=True)

            df['score'] = np.where(df['attribute'] == 'factor', np.nan, df['score'])
            df['grade'] = np.where(df['attribute'] == 'factor', np.nan, df['grade'])
            df['grade_url'] = np.where(df['attribute'] == 'factor', np.nan, df['grade_url'])

            df['domain'] = np.where(df['attribute'] == 'finding', np.nan, df['domain'])
            if 'count' in df.columns:
                df['count'] = np.where(df['attribute'] == 'finding', np.nan, df['count'])

            # Renaming columns based on entity name
            df.rename(columns=(
                {'domain': 'score_domain', 'score': 'score_score', 'grade': 'score_grade',
                 'grade_url': 'score_grade_url',
                 'last30day_score_change': 'score_last30day_score_change', 'name': 'factor_name', 'type': 'issue_type',
                 'count': 'issue_count',
                 'severity': 'issue_severity', 'total_score_impact': 'issue_total_score_impact',
                 'feedback': 'finding_feedback',
                 'first_seen_time': 'finding_first_seen_time', 'last_seen_time': 'finding_last_seen_time',
                 'evidence': 'finding_evidence',
                 'data_source': 'finding_data_source', 'issue_id': 'finding_issue_id', 'analysis': 'finding_analysis',
                 'scheme': 'finding_scheme', 'final_url': 'finding_final_url', 'observations': 'finding_observations',
                 'analysis_description': 'finding_analysis_description', 'url': 'finding_url',
                 'full_name': 'finding_full_name', 'year': 'finding_year', 'users': 'finding_users',
                 'description': 'finding_description'}), inplace=True)

            df = df.drop(['_t', 'attribute'], axis=1)  # delete metadata cols

            pd.set_option('display.max_columns', None)
            if {'score_domain', 'score_score', 'score_grade', 'score_grade_url', 'score_last30day_score_change','factor_name'}.issubset(df.columns):
               cols = ['score_domain', 'score_score', 'score_grade', 'score_grade_url', 'score_last30day_score_change',
                    'factor_name']
            # Applying forward fill to selected columns
               df.loc[:, cols] = df.loc[:, cols].ffill()

            # Left join two dataframes on factor_name column
            df = df.merge(df2, on='factor_name', how='left')

            if 'issue_type' in df.columns:
                df['issue_type'] = df['issue_type'].replace([np.NaN], 'tea')

            if 'finding_issue_id' in df.columns:
                df['finding_issue_id'] = df['finding_issue_id'].replace([np.NaN], 'coffee')

            if 'issue_type' in df.columns:
                df = df.drop(df[(df['issue_type'] == 'tea') & (df['factor_score'] != 100) & (
                        df['finding_issue_id'] == 'coffee')].index)

            if 0 in df.columns:
                df = df.drop([0], axis=1)  # delete 0 col

            df.reset_index(inplace=True)

            if 'index' in df.columns:
                df = df.drop(['index'], axis=1)  # delete index col

            if 'issue_type' in df.columns:
                df['issue_type'] = df['issue_type'].replace(['tea'], None)

            if 'finding_issue_id' in df.columns:
                df['finding_issue_id'] = df['finding_issue_id'].replace(['coffee'], None)
                # Left join two dataframes on finding_issue_id column
                df = df.merge(df3, on='finding_issue_id', how='left')

            mask = (df.columns.isin(
                ['finding_feedback', 'finding_first_seen_time', 'finding_last_seen_time', 'finding_analysis',
                 'finding_scheme', 'finding_final_url', 'finding_issue_id', 'finding_analysis_description',
                 'finding_full_name', 'finding_year', 'finding_description', 'finding_users',
                 'finding_observations_last_seen_at', 'finding_observations_evidence',
                 'finding_observations_initial_url', 'finding_observations_final_url', 'finding_domain',
                 'finding_count']))
            cols_to_shift = df.columns[mask]
            df[cols_to_shift] = df[cols_to_shift].shift(-1)

            if 'issue_type' in df.columns:
                df['issue_type'] = df['issue_type'].replace([np.NaN], 'tea')

            if 'finding_issue_id' in df.columns:
                df['finding_issue_id'] = df['finding_issue_id'].replace([np.NaN], 'coffee')

            if 'issue_type' in df.columns:
                df = df.drop(df[(df['issue_type'] == 'tea') & (df['factor_score'] != 100) & (
                        df['finding_issue_id'] == 'coffee')].index)
                df = df.drop(df[(df['issue_type'] == 'tea') & (df['factor_name'] == 'social_engineering') & (
                        df['factor_score'] == 100) & (df['finding_issue_id'] == 'coffee')].index)
                df = df.drop(df[(df['issue_type'] == 'tea') & (df['factor_name'] == 'application_security') & (
                        df['factor_score'] != 100)].index)

                df['issue_type'] = df['issue_type'].replace(['tea'], None)

            if 'finding_issue_id' in df.columns:
                df['finding_issue_id'] = df['finding_issue_id'].replace(['coffee'], None)

            df.reset_index(inplace=True)
            if 'index' in df.columns:
                df = df.drop(['index'], axis=1)  # delete index col

            df = df.applymap(str)  # convert all pandas columns to datatype string
            df = df.drop(df[df['factor_name'] == 'nan'].index)  
            logging.debug("transform")

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
    google_cloud_options.job_name = "scorecard"
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
        BUCKET_NAME = config['SECURITY_SCORECARD']['OUTPUT_BUCKET']
        SERVICE_ACCOUNT_EMAIL = config["COMMON"]["GCP"]["SERVICE_ACCOUNT_EMAIL"]
        PROJECT_ID = config["COMMON"]['GCP']['PROJECT_ID']
        REGION = config["COMMON"]['GCP']['REGION']
        DATAFLOW_STAGING = config["COMMON"]['DATAFLOW']['STAGING_LOCATION']
        DATAFLOW_TEMP = config["COMMON"]['DATAFLOW']['TEMP_LOCATION']
        SUBNETWORK = config["COMMON"]['DATAFLOW']['SUBNETWORK']
        DATASET_ID = config["COMMON"]['BIGQUERY']['SCORECARD']['DATASET_ID']
        TABLE_ID = config["COMMON"]['BIGQUERY']['SCORECARD']['TABLE_ID']

    except (ValueError, KeyError) as e:
        logging.error("failed to read config variable, %s", e)
        raise KeyError(f"failed to read config variable, {e}")

    run()
    logging.info(f"Full scorecard dataflow execution completed successfully on {date.today()}")