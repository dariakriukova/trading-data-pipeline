import boto3
import os
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
from dotenv import load_dotenv

arg_date = "2022-11-16"
arg_date_dt = datetime.strptime(arg_date, "%Y-%m-%d").date() - timedelta(days=1)

load_dotenv()

# Initialize a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)

# Access the S3 resource
s3 = session.resource("s3")
bucket = s3.Bucket("xetra-1234")

# Fetching objects for the specified dates
objects = [
    obj
    for obj in bucket.objects.all()
    if datetime.strptime(obj.key.split("/")[0], "%Y-%m-%d").date() >= arg_date_dt
]

# Read the first CSV object to get the initial dataframe structure
csv_obj_init = (
    bucket.Object(key=objects[0].key).get().get("Body").read().decode("utf-8")
)
data = StringIO(csv_obj_init)
df_init = pd.read_csv(data)

# Initialize a list to store dataframes
df_list = [df_init]

# Append data from each object to the list
for obj in objects[1:]:  # start from the second object since the first is already read
    csv_obj = bucket.Object(key=obj.key).get().get("Body").read().decode("utf-8")
    data = StringIO(csv_obj)
    df = pd.read_csv(data)
    df_list.append(df)

# Concatenate all dataframes into one
df_all = pd.concat(df_list, ignore_index=True)

columns = [
    "ISIN",
    "Date",
    "Time",
    "StartPrice",
    "MaxPrice",
    "MinPrice",
    "EndPrice",
    "TradedVolume",
]
df_all = df_all.loc[:, columns]
df_all.dropna(inplace=True)  # drop rows with missing values

# Get opening price for ISIN and day
df_all["opening_price"] = (
    df_all.sort_values(by=["Time"])
    .groupby(["ISIN", "Date"])["StartPrice"]
    .transform("first")
)

# Get closing price for ISIN and day
df_all["closing_price"] = (
    df_all.sort_values(by=["Time"])
    .groupby(["ISIN", "Date"])["EndPrice"]
    .transform("last")
)

# Aggregations
df_all = df_all.groupby(["ISIN", "Date"], as_index=False).agg(
    opening_price_eur=("opening_price", "min"),
    closing_price_eur=("closing_price", "min"),
    minimum_price_eur=("MinPrice", "min"),
    maximum_price_eur=("MaxPrice", "max"),
    daily_traded_volume=("TradedVolume", "sum"),
)

# Percent change Prev Closing
df_all["prev_closing_price"] = (
    df_all.sort_values(by=["Date"]).groupby(["ISIN"])["closing_price_eur"].shift(1)
)
df_all["change_prev_closing_%"] = (
    (df_all["closing_price_eur"] - df_all["prev_closing_price"])
    / df_all["prev_closing_price"]
    * 100
)
df_all.drop(columns=["prev_closing_price"], inplace=True)
df_all = df_all.round(decimals=2)
df_all = df_all[df_all.Date >= arg_date]

print(df_all)
