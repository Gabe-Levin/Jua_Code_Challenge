# Jua_Code_Challenge

## Results

Results can be found at:
s3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day_wH3/

This data supports:

- filtering by timestamp
- filtering by H3

## Instructions for data download

https://github.com/planet-os/notebooks/blob/master/aws/era5-s3-via-boto.ipynb

## Windows instructions in case of pandas overloading file size

https://stackoverflow.com/questions/57507832/unable-to-allocate-array-with-shape-and-data-type

## More challenging package downloads

conda install -c conda-forge xarray dask netCDF4 bottleneck
conda install -c conda-forge python-dotenv
conda update -n base conda
