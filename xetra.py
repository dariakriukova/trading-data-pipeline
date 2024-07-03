import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id = os.getenv('aws_access_key_id'),
    aws_secret_access_key = os.getenv('aws_secret_access_key')
)

# Access the S3 resource
s3 = session.resource('s3')

# Specify the bucket and prefix
bucket_name = 'xetra-1234'
prefix = '2022-01-28/'

# Get the bucket
bucket = s3.Bucket(bucket_name)

# List objects in the specified prefix
for obj in bucket.objects.filter(Prefix=prefix):
    print(obj.key)
