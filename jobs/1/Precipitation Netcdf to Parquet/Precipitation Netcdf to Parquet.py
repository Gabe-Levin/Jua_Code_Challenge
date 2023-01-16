
import boto3
import botocore
import datetime
import pandas as pd
import xarray as xr

# Create client for connecting ERA5 S3 directly
era5_bucket = 'era5-pds'
client = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))

# Select date and variable of interest
date = datetime.date(2022,5,1)
var = 'precipitation_amount_1hour_Accumulation'

# File path patterns for remote S3 objects and corresponding local file
s3_data_ptrn = '{year}/{month}/data/{var}.nc'
data_file_ptrn = '{year}{month}_{var}.nc'

year = date.strftime('%Y')
month = date.strftime('%m')
s3_data_key = s3_data_ptrn.format(year=year, month=month, var=var)
data_file = data_file_ptrn.format(year=year, month=month, var=var)

# Extract netcdf file directly from ERA5 bucket
client.download_file(era5_bucket, s3_data_key, data_file)

# Use xarray to access the contents of the netcdf file
ds = xr.open_dataset(data_file)

# Cycle through each day of the month of May, 2022
for day in range(1,32):
    # Clear the filtered df
    df_filtered= pd.DataFrame()
    
    # Partition the data by day
    file_date = f'2022-05-{day:02d}'
    ds_filtered=ds.sel(time1=slice(file_date,file_date))
    
    # Assign the filtered data to a pandas df
    df_filtered = ds_filtered.to_dataframe()
    
    # Save the filtered df to s3 as a compressed parquet
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/raw_parquet_by_day/precipitation_{file_date}.parquet.gzip'
    df_filtered.to_parquet(path=s3_url, compression='gzip') 
