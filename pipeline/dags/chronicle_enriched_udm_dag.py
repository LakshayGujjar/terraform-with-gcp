#  Copyright 2022 Google LLC. This software is provided as is,
#  without warranty or representation for any use or purpose.
#  Your use of it is subject to your agreement with Google.

"""DAG to migrate enriched UDM data from chronicle BigQuery instance to NJCCIC BigQuery instance """

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow import AirflowException

from google.cloud import bigquery

from dependencies.logging_utils import setup_logger
from dependencies.yaml_reader_utils import config_reader

from datetime import datetime, date, timedelta
import datetime as dt
import pendulum
import logging
import subprocess
import shlex
import os
import json

logger = setup_logger(logging.getLogger(__name__))
logger.info('Chronicle Enriched UDM pipeline execution started')

category = "CHRONICLE_ENRICHED_UDM"
logger.debug("Reading variables from YAML configuration file")
config_data = config_reader(category)

category_common = "COMMON"
common_config_data = config_reader(category_common)

try:
    # variable extraction
    source_project = config_data['SOURCE_PROJECT']
    destination_project = config_data['DESTINATION_PROJECT']
    source_dataset = config_data['SOURCE_DATASET']
    destination_dataset = config_data['DESTINATION_DATASET']
    interval = config_data['DAILY_SCHEDULE_INTERVAL']
    udm_events_table = config_data['UDM_EVENTS_TABLE']
    udm_events_aggregates_table = config_data['UDM_EVENTS_AGGREGATES_TABLE']
    ingestion_stats_table = config_data['INGESTION_STATS_TABLE']
    entity_graph_table = config_data['ENTITY_GRAPH_TABLE']
    dag_owner = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_OWNER']
    dag_retries = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_RETRIES']
    dag_start_date = common_config_data['AIRFLOW']['DEFAULT_DAG_ARGS_START_DATE']

except KeyError as key_error:
    logger.error("failed to read config variable: {}".format(key_error))
    logging.critical("Chronicle Enriched UDM pipeline failed due to Key error: {}".format(key_error))
    raise AirflowException(f"failed to read config variable: {key_error}")

except Exception as err:
    logger.error("failed in extracting config variables due to error: {}".format(err))
    logging.critical("Chronicle Enriched UDM pipeline failed in extracting config variables due to error: {}".format(err))
    raise AirflowException(f"failed in extracting config variable due to error: {err}")

default_args = {
    'owner': dag_owner,
    'start_date':dag_start_date,
    'retries': dag_retries,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff' : True,
}

'''Functions START'''
def schema_compare(table_id, ti):
    """Function for comparing BigQuery schemas"""
    global njccic_bq_client
    try:
        logger.info("Enriched UDM DAG task: schema_compare started for table: {}".format(table_id))
        '''CHRONICLE Instance'''
        chronicle_bq_client = bigquery.Client(project=source_project)
        chronicle_bq_dataset = bigquery.DatasetReference(chronicle_bq_client.project, source_dataset)
        chronicle = bigquery.TableReference(chronicle_bq_dataset, table_id)
        chronicle_table = chronicle_bq_client.get_table(chronicle)
        ti.xcom_push(key='chronicle_table_xcom', value=chronicle_table)
        chronicle_table_schema = chronicle_table.schema

        '''NJCCIC Instance'''
        njccic_bq_client = bigquery.Client(project=destination_project)
        njccic_bq_dataset = bigquery.DatasetReference(njccic_bq_client.project, destination_dataset)
        njccic = bigquery.TableReference(njccic_bq_dataset, table_id)
        njccic_table = njccic_bq_client.get_table(njccic)
        ti.xcom_push(key='njccic_table_xcom', value=njccic_table)
        njccic_table_schema = njccic_table.schema

        if chronicle_table_schema == njccic_table_schema:
            logger.debug("Schema matched for table: {}".format(table_id))
            # Pushing True to function:schema_evolution if schema matched for source and destination table
            ti.xcom_push(key='evolution_xcom', value=True)

        else:
            logger.debug("Schema mismatched for table: {}".format(table_id))
            # Pushing False to function:schema_evolution if schema mismatched for source and destination table
            ti.xcom_push(key='evolution_xcom', value=False)

        logger.info("Enriched UDM DAG task:schema_compare executed successfully for table: {}".format(table_id))

    except Exception as err:
        logger.error("failed in task: schema_compare due to error: {}".format(err))
        logging.critical(
            "Chronicle Enriched UDM pipeline failed in task: schema_compare for table: {0} due to error: {1}".format(
                table_id, err))
        raise AirflowException(f"failed in task: schema_compare due to error: {err}")


def schema_evolution(table_id, ti):
    """Function for schema evolution i.e. Updating new columns added to the source table automatically"""
    try:
        logger.info("Enriched UDM DAG task: schema_evolution started for table: {}".format(table_id))
        #Pulling xcom value for key evolution_xcom
        compare_return = ti.xcom_pull(key='evolution_xcom',task_ids='processing_tasks_{}.schema_compare'.format(table_id))

        chronicle_table = ti.xcom_pull(key='chronicle_table_xcom', task_ids='processing_tasks_{}.schema_compare'.format(table_id))
        logger.info(f"chronicle: {chronicle_table}")

        njccic_table = ti.xcom_pull(key='njccic_table_xcom',task_ids='processing_tasks_{}.schema_compare'.format(table_id))
        logger.info(f"njccic: {njccic_table}")

        #Checking if schema_compare returned False
        table_partition_map = {'entity_graph': 'daily', 'udm_events': 'hourly', 'udm_events_aggregates': 'hourly','ingestion_stats': 'hourly'}

        if not compare_return:
            logger.debug("Updating Schema for table: {}".format(table_id))
            # Pulling xcom variables for chronicle_table_xcom, njccic_table_xcom and njccic_bq_client_xcom

            chronicle_table_schema = list(chronicle_table.schema)
            #creating an empty list to insert new columns added at source
            njccic_table_new_schema = list()
            try:
                #Updating schema of destination table (NJCCIC BQ INSTANCE)
                for new_column in chronicle_table_schema:
                    njccic_table_new_schema.append(bigquery.SchemaField(
                        new_column.name,
                        new_column.field_type,
                        fields=new_column.fields,
                        mode=new_column.mode))

                njccic_table.schema = njccic_table_new_schema
                njccic_bq_client.update_table(njccic_table, ['schema'])
                logger.debug("Schema updated for table: {}".format(table_id))

            except Exception as err:
                logger.error("Failed to update schema in task schema_evolution for table: {0} due to error: {1}".format(table_id,err))
                logging.critical("Chronicle Enriched UDM pipeline failed in task: schema_evolution for table: {0} after attempting updating schema due to error: {1}".format(
                        table_id, err))
                raise AirflowException(f"Failed to update schema in task schema_evolution for table: {table_id}")

            ti.xcom_push(key='migrate_xcom', value=table_partition_map.get(table_id))

        #Skipping editing schema as schema matched for source and destination table
        else:
            ti.xcom_push(key='migrate_xcom', value=table_partition_map.get(table_id))

        logger.info("Enriched UDM DAG task:schema_evolution executed successfully for table: {}".format(table_id))

    except Exception as err:
        logger.error("Failed in task schema_evolution for table: {0} due to error: {1}".format(table_id, err))
        logging.critical(
            "Chronicle Enriched UDM pipeline failed in task: schema_evolution for table: {0} due to error: {1}".format(
                table_id, err))
        raise AirflowException(f"Failed in task schema_evolution for table: {table_id}")

def migrate_data(table_id, ti):
    """Function for Migrating data from BQ table to BQ table using BQ CP cmd"""
    try:
        logger.info("Enriched UDM DAG task: migrate_data started for table: {}".format(table_id))
        # Pulling xcom value for key migrate_xcom
        se_return = ti.xcom_pull(key='migrate_xcom',task_ids='processing_tasks_{}.schema_evolution'.format(table_id))
        #Checking if se_return is hour_partition or day_partition
        if se_return == 'hourly':
            try:
                logger.info("Enriched UDM DAG task: migrate_hour_partition_table executed started for table: {}".format(table_id))
                DAYS = 179 # current_date minus 179 days (for 180 day expiration table)
                partition_date = (date.today() - timedelta(DAYS)).strftime('%Y%m%d')
                source = source_project + ":" + source_dataset + "." + table_id
                logger.debug("source: {}".format(source))
                destination = destination_project + ":" + destination_dataset + "." + table_id
                logger.debug("destination: {}".format(destination))
                # creating a partition range ranging from [00 to 23] where 00 = 12AM, 01 = 1AM ... 23 = 11PM
                partition_hours = [dt.time(i).strftime('%H') for i in range(24)]
                #Copying each hour partition for a day
                for partition_hour in partition_hours:
                    cmd = """bq --location=us cp -f=true """ + "'" + source + "$" + partition_date + partition_hour + "'" + " " + "'" + destination + "$" + partition_date + partition_hour + "'"
                    # cmd="bq --location=us cp {} {}".format(source,destination)
                    logger.debug("COMMAND:{}".format(cmd))
                    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                    stdout, stderr = proc.communicate()
                    # convert response to list
                    logger.debug("std out: {}".format(stdout))
                    logger.debug("std err: {}".format(stderr))
                    logger.info("Enriched UDM DAG task: migrate_hour_partition_table executed successfully for table: {}".format(table_id))

            except OSError as error_os:
                log = {
                    "OSError_no": error_os.errno,
                    "OSError_str": error_os.strerror,
                }
                logger.error(json.dumps(log))
                raise AirflowException(f"shell failed to find the requested application, {error_os}")

            except ValueError as error_value:
                raise AirflowException(f"Popen is called with invalid arguments, {error_value}")

            except Exception as err:
                logger.error("failed in task: migrate_hour_partition_table due to error: {}".format(err))
                logging.critical(
                    "Chronicle Enriched UDM pipeline failed in task: migrate_hour_partition_table for table: {0} due to error: {1}".format(
                        table_id, err))
                raise AirflowException(f"failed in task: migrate_hour_partition_table due to error: {err}")

        elif se_return == 'daily':
            try:
                logger.info("Enriched UDM DAG task: migrate_day_partition_table executed started for table: {}".format(table_id))
                DAYS = 58 # current_date minus 58 days (for 59 day expiration table)
                partition_date = (date.today() - timedelta(DAYS)).strftime('%Y%m%d')
                source = source_project + ":" + source_dataset + "." + table_id
                logger.debug("source: {}".format(source))
                destination = destination_project + ":" + destination_dataset + "." + table_id
                logger.debug("destination: {}".format(destination))
                #Copying day partition for a day
                cmd = """bq --location=us cp -f=true """ + "'" + source + "$" + partition_date + "'" + " " + "'" + destination + "$" + partition_date + "'"
                # cmd="bq --location=us cp {} {}".format(source,destination)
                logger.debug("COMMAND:{}".format(cmd))
                proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                stdout, stderr = proc.communicate()
                logger.debug("std out: {}".format(stdout))
                logger.debug("std err: {}".format(stderr))
                logger.info("Enriched UDM DAG task: migrate_day_partition_table executed successfully for table: {}".format(table_id))

            except OSError as error_os:
                log = {
                    "OSError_no": error_os.errno,
                    "OSError_str": error_os.strerror,
                }
                logger.error(json.dumps(log))
                raise AirflowException(f"shell failed to find the requested application, {error_os}")

            except ValueError as error_value:
                raise AirflowException(f"Popen is called with invalid arguments, {error_value}")

            except Exception as err:
                logger.error("failed in task: migrate_day_partition_table due to error: {}".format(err))
                logging.critical(
                    "Chronicle Enriched UDM pipeline failed in task: migrate_day_partition_table for table: {0} due to error: {1}".format(
                        table_id, err))
                raise AirflowException(f"failed in task: migrate_day_partition_table due to error: {err}")

        else:
            logger.error("se_return value is not hour partition or day_partition")
            logging.critical("Chronicle Enriched UDM pipeline failed in task: migrate_data for table: {0} as se_return value is {1}".format(table_id, se_return))
            raise AirflowException("Chronicle Enriched UDM pipeline failed in task: migrate_data for table: {0} as se_return value is {1}".format(table_id, se_return))

    except Exception as err:
        logger.error("Failed in task migrate_data for table: {0} due to error: {1}".format(table_id, err))
        logging.critical("Chronicle Enriched UDM pipeline failed in task: migrate_data for table: {0} due to error: {1}".format(table_id, err))
        raise AirflowException(f"Failed in task migrate_data for table: {table_id}")
'''Functions END'''

with DAG('chronicle_enriched_udm_dag',
         schedule_interval='0 1 * * *',
         tags=["Chronicle-Enriched-UDM-Migration-DAG"],
         description="DAG for data migration of enriched udm source",
         default_args=default_args,
         ) as dag:

    # start dummy operator
    opr_start = DummyOperator(task_id='START')

    table_names = [udm_events_table, udm_events_aggregates_table, ingestion_stats_table, entity_graph_table]

    groups = []
    # Three operators: compare, evolution and migrate for four tables
    for table_name in table_names:
        with TaskGroup(group_id=f'processing_tasks_{table_name}') as processing_tasks:

            opr_schema_compare = PythonOperator(
                task_id='schema_compare',
                python_callable=schema_compare,
                op_kwargs={'table_id': table_name}
            )

            opr_schema_evolution = PythonOperator(
                task_id='schema_evolution',
                python_callable=schema_evolution,
                op_kwargs={'table_id': table_name}
            )

            opr_migrate_data = PythonOperator(
                task_id='migrate_data',
                python_callable=migrate_data,
                op_kwargs={'table_id': table_name}
            )

            opr_schema_compare >> opr_schema_evolution >> opr_migrate_data
            groups.append(processing_tasks)

    # end dummy operator
    opr_end = DummyOperator(task_id='END')

    #stream
    opr_start >> groups
    groups >> opr_end

    logger.debug(f"Chronicle Enriched UDM pipeline execution completed successfully on {date.today()}")