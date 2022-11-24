#  Copyright 2022 Google LLC. This software is provided as is,
#  without warranty or representation for any use or purpose.
#  Your use of it is subject to your agreement with Google.

"""
utils for google secret manager key access
"""
import json
import logging

from google.cloud import secretmanager

def get_secret(project_id, secret_id, version):
    try:
        """
        Get API Secret access token from the given gcp secret manager.
        """
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()
        # Build the resource name of the secret version.
        name = "projects/{}/secrets/{}/versions/{}".format(project_id, secret_id, version)
        # Access the secret version.
        response = client.access_secret_version(name=name)
        # Return the decoded payload.
        response_string = response.payload.data.decode('UTF-8')
        return response_string

    except Exception as error:
        logging.error("failed::{}".format(error))