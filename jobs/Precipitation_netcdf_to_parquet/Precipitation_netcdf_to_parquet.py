
import boto3
import botocore
import datetime
import os.path
import pandas as pd
import xarray as xr

era5_bucket = 'era5-pds'

# AWS access / secret keys required
# s3 = boto3.resource('s3')
# bucket = s3.Bucket(era5_bucket)

# No AWS keys required
client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

# select date and variable of interest
date = datetime.date(2022,5,1)
var = 'precipitation_amount_1hour_Accumulation'

# file path patterns for remote S3 objects and corresponding local file
s3_data_ptrn = '{year}/{month}/data/{var}.nc'
data_file_ptrn = '{year}{month}_{var}.nc'

year = date.strftime('%Y')
month = date.strftime('%m')
s3_data_key = s3_data_ptrn.format(year=year, month=month, var=var)
data_file = data_file_ptrn.format(year=year, month=month, var=var)

if not os.path.isfile(data_file): # check if file already exists
    print("Downloading %s from S3..." % s3_data_key)
    client.download_file(era5_bucket, s3_data_key, data_file)

ds = xr.open_dataset(data_file)

# df_filtered= pd.DataFrame()
# file_date = f'2022-05-05'
# ds_filtered=ds.sel(time1=slice(file_date,file_date))
# df_filtered = ds_filtered.to_dataframe()

# s3_url = f's3://jua-code-challenge-data/data/precipitation_data/raw_parquet_by_day/precipitation_{file_date}.parquet.gzip'
# df_filtered.to_parquet(path=s3_url, compression='gzip') 

for day in range(1,32):
    df_filtered= pd.DataFrame()
    file_date = f'2022-05-{day:02d}'
    ds_filtered=ds.sel(time1=slice(file_date,file_date))
    df_filtered = ds_filtered.to_dataframe()

    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/raw_parquet_by_day/precipitation_{file_date}.parquet.gzip'
    df_filtered.to_parquet(path=s3_url, compression='gzip') 
