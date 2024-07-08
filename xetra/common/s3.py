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
from io import StringIO, BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


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

    def read_csv_to_df(self, key: str, encoding: str = "utf-8", sep: str = ","):
        """
        reading a csv file from the S3 bucket and returning a dataframe

        :param key: key of the file that should be read
        :encoding: encoding of the data inside the csv file
        :sep: seperator of the csv file

        returns:
          data_frame: Pandas DataFrame containing the data of the csv file
        """
        self._logger.info(
            "Reading file %s/%s/%s", self.endpoint_url, self._bucket.name, key
        )
        csv_obj = self._bucket.Object(key=key).get().get("Body").read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)
        return data_frame

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        writing a Pandas DataFrame to S3
        supported formats: .csv, .parquet

        :data_frame: Pandas DataFrame that should be written
        :key: target key of the saved file
        :file_format: format of the saved file
        """
        if data_frame.empty:
            self._logger.info("The dataframe is empty! No file will be written!")
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info(
            "The file format %s is not " "supported to be written to s3!", file_format
        )
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info(
            "Writing file to %s/%s/%s", self.endpoint_url, self._bucket.name, key
        )
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
