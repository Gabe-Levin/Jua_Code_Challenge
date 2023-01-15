import datetime
from awswrangler import s3
import pandas as pd
import h3

for day in range(1,32):
    file_date = f'2022-05-{day:02d}'
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day/precipitation_{file_date}.parquet.gzip'

    dfs = s3.read_parquet(path = s3_url, chunked=True)
    
    df_by_day = pd.DataFrame()
    for df in dfs:
        df['h3_index'] = df.apply(lambda row: h3.geo_to_h3(row['lat'], row['lon'], 2), axis=1)

        df_by_day = df_by_day.append(df,ignore_index=True)
    
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day_wH3/precipitation_{file_date}.parquet.gzip'
    s3.to_parquet(df=df_by_day, path=s3_url, compression='gzip')