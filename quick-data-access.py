import boto3
import os
from datetime import datetime, timedelta
from io import StringIO, BytesIO
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Adapter Layer


def read_csv_to_df(bucket, key, decoding="utf-8", sep=","):
    csv_obj = bucket.Object(key=key).get().get("Body").read().decode(decoding)
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=sep)
    return df


def write_df_to_s3(bucket, df, key):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), Key=key)
    return True


def list_files_in_prefix(bucket, prefix):
    files = [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
    return files


# Application Layer


def extract(bucket, date_list):
    files = [key for date in date_list for key in list_files_in_prefix(bucket, date)]
    df = pd.concat([read_csv_to_df(bucket, obj) for obj in files], ignore_index=True)
    return df


def transform_report1(df, columns, arg_date):
    df = df.loc[:, columns]
    df.dropna(inplace=True)
    df["opening_price"] = (
        df.sort_values(by=["Time"])
        .groupby(["ISIN", "Date"])["StartPrice"]
        .transform("first")
    )
    df["closing_price"] = (
        df.sort_values(by=["Time"])
        .groupby(["ISIN", "Date"])["StartPrice"]
        .transform("last")
    )
    df = df.groupby(["ISIN", "Date"], as_index=False).agg(
        opening_price_eur=("opening_price", "min"),
        closing_price_eur=("closing_price", "min"),
        minimum_price_eur=("MinPrice", "min"),
        maximum_price_eur=("MaxPrice", "max"),
        daily_traded_volume=("TradedVolume", "sum"),
    )
    df["prev_closing_price"] = (
        df.sort_values(by=["Date"]).groupby(["ISIN"])["closing_price_eur"].shift(1)
    )
    df["change_prev_closing_%"] = (
        (df["closing_price_eur"] - df["prev_closing_price"])
        / df["prev_closing_price"]
        * 100
    )
    df.drop(columns=["prev_closing_price"], inplace=True)
    df = df.round(decimals=2)
    df = df[df.Date >= arg_date]
    return df


def load(bucket, df, trg_key, trg_format):
    key = trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + trg_format
    write_df_to_s3(bucket, df, key)
    return True


def etl_report1(
    src_bucket, trg_bucket, date_list, columns, arg_date, trg_key, trg_format
):
    df = extract(src_bucket, date_list)
    df = transform_report1(df, columns, arg_date)
    load(trg_bucket, df, trg_key, trg_format)
    return True


# Application Layer - not core


def return_date_list(bucket, arg_date, src_format):
    min_date = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
    today = datetime.today().date()
    return_date_list = [
        (min_date + timedelta(days=x)).strftime(src_format)
        for x in range(0, (today - min_date).days + 1)
    ]
    return return_date_list


# Main function entrypoint
def main():
    # Parameters/Configurations
    # Later read config
    arg_date = "2022-12-27"
    src_format = "%Y-%m-%d"
    src_bucket = "xetra-1234"
    trg_bucket = "xetra-1234-final"
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
    trg_key = "xetra_daily_report_"
    trg_format = ".parquet"

    # Init
    session = boto3.Session(
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )
    # Access the S3 resource
    s3 = session.resource("s3")
    bucket_src = s3.Bucket(src_bucket)
    bucket_trg = s3.Bucket(trg_bucket)

    # Run application
    date_list = return_date_list(bucket_src, arg_date, src_format)
    etl_report1(
        bucket_src, bucket_trg, date_list, columns, arg_date, trg_key, trg_format
    )


# Run
main()

# Reading the uploaded file
session = boto3.Session(
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)
s3 = session.resource("s3")
trg_bucket = "xetra-1234-final"
bucket_trg = s3.Bucket(trg_bucket)
for obj in bucket_trg.objects.all():
    print(obj.key)

prq_obj = (
    bucket_trg.Object(key="xetra_daily_report_20240704_135550.parquet")
    .get()
    .get("Body")
    .read()
)
data = BytesIO(prq_obj)
df_report = pd.read_parquet(data)
print(df_report)
