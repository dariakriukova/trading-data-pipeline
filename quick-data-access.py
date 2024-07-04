import boto3
import os
from dotenv import load_dotenv
from io import StringIO
import pandas as pd

load_dotenv()

# Initialize a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id = os.getenv('aws_access_key_id'),
    aws_secret_access_key = os.getenv('aws_secret_access_key')
)

# Access the S3 resource
s3 = session.resource('s3')
bucket = s3.Bucket('xetra-1234')

# Fetching objects for the specified dates
bucket_obj1 = bucket.objects.filter(Prefix='2022-03-15/')
bucket_obj2 = bucket.objects.filter(Prefix='2022-03-16/')

# Combine objects from both dates
objects = [obj for obj in bucket_obj1] + [obj for obj in bucket_obj2]

# Read the first CSV object to get the initial dataframe structure
csv_obj_init = bucket.Object(key=objects[0].key).get().get('Body').read().decode('utf-8')
data = StringIO(csv_obj_init)
df_init = pd.read_csv(data)

# Initialize a list to store dataframes
df_list = [df_init]

# Append data from each object to the list
for obj in objects[1:]:  # start from the second object since the first is already read
    csv_obj = bucket.Object(key=obj.key).get().get('Body').read().decode('utf-8')
    data = StringIO(csv_obj)
    df = pd.read_csv(data)
    df_list.append(df)

# Concatenate all dataframes into one
df_all = pd.concat(df_list, ignore_index=True)


columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
df_all = df_all.loc[:, columns]
df_all.dropna(inplace=True)  # drop rows with missing values
print(df_all)