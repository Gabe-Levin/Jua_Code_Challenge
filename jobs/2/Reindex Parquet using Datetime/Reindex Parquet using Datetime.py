
from awswrangler import s3
import pandas as pd

# Cycle through each day of the month of May, 2022
for day in range(1,32):
    file_date = f'2022-05-{day:02d}'
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/raw_parquet_by_day/precipitation_{file_date}.parquet.gzip'

    # Read parquet files by day using chunked=True to reduce size of dataframes, greatly reducing memory requirements for processing
    dfs = s3.read_parquet(path = s3_url, chunked=True)
    
    # For each day, (1) reindex by time field of the chuncked dataframes and convert to datetime, (2) combine them, and (3) then save the result to s3
    df_by_day = pd.DataFrame()
    for df in dfs:
        # Reset index so the time1 is only index and convert to datetime
        df_time_index = df.reset_index(level=['lon','lat','nv'])
        df_time_index.index = pd.to_datetime(df_time_index.index)
        
        # Combine chunked dataframes
        df_by_day = df_by_day.append(df,ignore_index=True)
    
    # Save result to a different s3 directory
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day/precipitation_{file_date}.parquet'
    s3.to_parquet(df=df_by_day, path=s3_url)