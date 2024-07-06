"""
Module for interacting with S3 buckets using boto3.

This module provides the S3BucketConnector class, which allows for listing
files in an S3 bucket with a given prefix. It uses the boto3 library to
interface with AWS S3 and reads AWS credentials from environment variables.

Usage:
    from s3_connector import S3BucketConnector

    s3_connector = S3BucketConnector(bucket='my-bucket', endpoint_url='https://s3.amazonaws.com')
    files = s3_connector.list_files_in_prefix(prefix='my-prefix/')
"""

import os
import logging
import boto3
from dotenv import load_dotenv

load_dotenv()


class S3BucketConnector:
    """
    Class for interacting with S3 buckets
    """

    def __init__(self, bucket: str, endpoint_url: str = None):
        """
        Initialize the S3BucketConnector object.

        Parameters:
        bucket (str): Name of the S3 bucket.
        endpoint_url (str): URL of the S3 endpoint.
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"),
        )
        self._s3 = self.session.resource(service_name="s3", endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        List files in the S3 bucket with a given prefix.

        Parameters:
        prefix (str): Prefix for the files to list.

        Returns:
        files: List of file names in the bucket with the given prefix.
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
