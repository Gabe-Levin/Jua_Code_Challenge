# Jua_Code_Challenge

## TL:DR

Results can be found at:
s3://jua-code-challenge-data/data/precipitation_data/reindexed_parquet_by_day_wH3/

## Introduction

This repo contains the code for the Jan, 2023 Jua Geospatial Data Engineer coding challenge.

The majority of the assignment was completed using AWS Glue, whose jobs can be found in the 'jobs/' folder. To view the jobs, check the python scripts in 'jobs' folder within this repo, or follow the steps in the correspond section below to view them in the AWS console where they are being run.

The jupyter notebooks found in the "data_exploration/" folder are used for the initial data exploration and ETL design process. There may not be much value in running the notebooks now, but they are included to describe the development workflow and to demonstrate some software development best practices (i.e. commit logs, env files, etc.). To run the notebooks check the corresponding section below.

## ETL Outline

The ETL process is divided into three steps (or jobs). Each step is assigned to a different AWS Glue Job, which is executed within an AWS Glue Job python script. The names of the three jobs listed below correspond to the name of the AWS Glue jobs:

1.  "Precipitation Netcdf to Parquet"

    - In this job, data is extracted from ERA5 S3 as NetCDF
    - The data is accessed using xarray
    - And then exported by day as parquet files to my s3 bucket

2.  "Reindex Parquet using Datetime"

    - In this job, data is extracted from my s3 as parquet
    - The data is converted into chuncked pandas dataframes using aws wrangler
    - The dataframes are reindexed from multi-index to single-index (to support dask if later integrated)
    - The index is then converted into a datetime object
    - The dataframes are grouped by day and then saved as a compressed parquet file using gzip compression

3.  "Add H3 to Reindexed Parquet"
    - In this job, data is extracted from my s3 containing the results from job 2
    - The data is converted into chuncked pandas dataframes again and a H3 index is added to the dataframes using the h3 package.
    - Finally, the dataframes are again grouped by day and saved as parquet files

## Accessing AWS Glue Jobs in AWS Console

1. Sign in with the url, username, and password provided with the submission of this assignment.
2. This account has access to my AWS account, but with very specific IAM policies. You will only have access to specific S3 buckets, AWS Glue Jobs and Workflows used for this assignment.
3. Set your region to 'eu-central-1'.
4. To access the AWS Glue Jobs and workflowm, type "AWS Glue" into the search bar, clicking on "AWS Glue", and selecting "Jobs" or "workflows" accordingly on the left-side menu.
5. Similarly, to access the S3 bucket, type "S3" into the search bar, and click on "S3". The assignment data is found in the bucket with the name "jua-code-challenge-data".

## Instruction for running the Jupyter Notebooks

1. Create a conda env (I used miniconda)
2. Download requirements from the requirements.txt file
3. Create a .env file and add the corresponding environment variables following the structure from the env.example file
4. Run the notebooks

Note: as mentioned above, these notebooks were used for data exploration and workshoping the ETL scripts that are doing all the work in AWS Glue.

## References

Instructions for ERA5 source data download: https://github.com/planet-os/notebooks/blob/master/aws/era5-s3-via-boto.ipynb
