#  Copyright 2022 Google LLC. This software is provided as is,
#  without warranty or representation for any use or purpose.
#  Your use of it is subject to your agreement with Google.
""" utils for yaml config file access """
import yaml
from yaml.loader import SafeLoader
import os

import logging
from dependencies.logging_utils import setup_logger

logger = setup_logger(logging.getLogger(__name__))

def config_reader(category:str):
    """
    function to read data from yaml file
    """
    try:
        config_path =os.environ.get("CONFIG_LOCATION","/home/airflow/gcs/dags/config.yaml")
        with open(config_path) as f:
            logger.info("Opened YAML config file")
            configs = yaml.load(f, Loader=SafeLoader)
            logger.info("Opened YAML config file")
            data = configs[category]
            logger.info("Accessed a category from YAML config file")
            f.close()

    except yaml.YAMLError as err:
        logger.error("Error while parsing YAML file:".format(err))
        raise yaml.YAMLError(f"Error while parsing YAML file:", {err})

    logger.debug("YAML config reader was successful")
    return data