
from awswrangler import s3
import pandas as pd
import h3

# Cycle through each day of the month of May, 2022
for day in range(1,32):
    file_date = f'2022-05-{day:02d}'
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day/precipitation_{file_date}.parquet.gzip'
    
    # Read parquet files by day using chunked=True to reduce size of dataframes, greatly reducing memory requirements for processing
    dfs = s3.read_parquet(path = s3_url, chunked=True)
    
    # For each day, (1) add h3 to the chuncked dataframes, (2) combine them, and (3) then save the result to s3
    df_by_day = pd.DataFrame()
    
    for df in dfs:
        # Add h3 indexing field
        df['h3_index'] = df.apply(lambda row: h3.geo_to_h3(row['lat'], row['lon'], 2), axis=1)
        
        # Combine chucked dataframes
        df_by_day = df_by_day.append(df,ignore_index=True)
    
    # Save result to a different s3 directory
    s3_url = f's3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day_wH3/precipitation_{file_date}.parquet'
    s3.to_parquet(df=df_by_day, path=s3_url)