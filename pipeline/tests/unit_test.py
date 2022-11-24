#  Copyright 2022 Google LLC. This software is provided as is,
# without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.

"""Unit test for composer dags"""
from unittest import mock
import pytest
import pandas as pd
import numpy as np

import google.api_core.exceptions
from airflow.models import DagBag
from airflow.exceptions import DagNotFound

from dependencies.yaml_reader_utils import config_reader
from dependencies.logging_utils import setup_logger

from .internal_unit_test import assert_has_valid_dag

def test_yaml_reader_feature():
    

    common_data = config_reader("GCP")
    assert common_data['COMMON']['PROJECT_ID'] == 'njccic-sdw'
    assert common_data['COMMON']['REGION'] == 'us-east4'

    enriched_data = config_reader("CHRONICLE_ENRICHED_UDM")
    assert enriched_data['SOURCE_PROJECT'] == 'chronicle-njc'
    assert enriched_data['DESTINATION_PROJECT'] == 'njccic-sdw'
    assert enriched_data['SOURCE_DATASET'] == 'datalake'
    assert enriched_data['DESTINATION_DATASET'] == 'chronicle_udm_enriched'

    
def test_chronicle_udm_dag():
    """
    Assert that a module contains a valid DAG
    """
    from dags import chronicle_enriched_udm_dag

    assert_has_valid_dag(chronicle_enriched_udm_dag)



def test_dagbag():
    """DAG Integrity Test
    This function will test for Missing required arguments (such as forgotten DAG ID), Duplicate
    DAG ids, Cycles in DAGs and does each DAG have a tag
    """
    with open("../tests/data/airflow_configs.yaml", encoding='utf-8') as config_file:
        config_file=config_file.read().encode('utf-8')
        with mock.patch(
            "builtins.open", mock.mock_open(read_data=config_file)
        ) as _:
            dag_bag = DagBag(include_examples=False)
            assert not dag_bag.import_errors
            for _ , dag in dag_bag.dags.items():
                assert dag.tags #Assert dag.tags is not empty