#  Copyright 2022 Google LLC. This software is provided as is,
# without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.
"""
This function is used to setup logging.
"""
import logging
import sys


def setup_logger(logger):
    """
    This function will initialize logging.
    """
    logger.propagate = False
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(lambda r: r.levelno < logging.WARNING)
    logger.addHandler(stdout_handler)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.addFilter(lambda r: r.levelno >= logging.WARNING)
    logger.addHandler(stderr_handler)
    logger.setLevel(logging.DEBUG)
    return logger