import datetime
from awswrangler import s3
import pandas as pd

for day in range(1,32):
    file_date = f'2022-05-{day:02d}'
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/raw_parquet_by_day/precipitation_{file_date}.parquet.gzip'

    dfs = s3.read_parquet(path = s3_url, chunked=True)
    
    df_by_day = pd.DataFrame()
    for df in dfs:
        # Reset index so the time1 is only index
        df_time_index = df.reset_index(level=['lon','lat','nv'])
        pd.to_datetime(df_time_index.index)

        df_by_day = df_by_day.append(df_time_index,ignore_index=True)
    
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day_wH3/precipitation_{file_date}.parquet.gzip'
    s3.to_parquet(df=df_by_day, path=s3_url, compression='gzip')