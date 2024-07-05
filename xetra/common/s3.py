"""Connector and methods accessing S3"""

import os
import boto3
from dotenv import load_dotenv
import logging

load_dotenv()


class S3BucketConnector:
    """
    Class for interacting with S3 buckets
    """

    def __init__(
        self, access_key: str, secret_key: str, bucket: str, endpoint_url: str
    ):
        """
        Initialize the S3BucketConnector object.

        Parameters:
        access_key (str): AWS access key.
        secret_key (str): AWS secret key.
        bucket (str): Name of the S3 bucket.
        end_point_url (str): URL of the S3 endpoint.
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"),
        )
        self._s3 = self.session.resource(service_name="s3", endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
