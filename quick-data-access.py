import boto3
import os
from datetime import datetime, timedelta
from io import StringIO, BytesIO
import pandas as pd
from dotenv import load_dotenv

arg_date = "2022-12-27"
src_format = "%Y-%m-%d"
src_bucket = "xetra-1234"
bucket_target = "xetra-1234-final"
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
key = "xetra_daily_report_" + datetime.today().strftime("%Y%m%d_%H%M%S") + ".parquet"

arg_date_dt = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)

load_dotenv()

# Initialize a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)

# Access the S3 resource
s3 = session.resource("s3")
bucket = s3.Bucket(src_bucket)

# Fetching objects for the specified dates
objects = [
    obj
    for obj in bucket.objects.all()
    if datetime.strptime(obj.key.split("/")[0], src_format).date() >= arg_date_dt
]


def csv_to_df(filename):
    csv_obj = bucket.Object(key=filename).get().get("Body").read().decode("utf-8")
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=",")
    return df


df_all = pd.concat([csv_to_df(obj.key) for obj in objects], ignore_index=True)


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

## Save to S3
out_buffer = BytesIO()
df_all.to_parquet(out_buffer, index=False)
bucket_target = s3.Bucket(bucket_target)
bucket_target.put_object(Body=out_buffer.getvalue(), Key=key)

# Reading the uploaded file
for obj in bucket_target.objects.all():
    print(obj.key)

prq_obj = (
    bucket_target.Object(key="xetra_daily_report_20240704_105225.parquet")
    .get()
    .get("Body")
    .read()
)
data = BytesIO(prq_obj)
df_report = pd.read_parquet(data)
print(df_report)
