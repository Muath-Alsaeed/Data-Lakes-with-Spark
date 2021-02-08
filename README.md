Project: Data Lake with Spark
------------------------
Introduction
-------------------------
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

goal of the project:
-------------------
 building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.
 
Project Datasets
-----------------


Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

Schema 
---------------------
![image](https://user-images.githubusercontent.com/52973147/100525293-948a9200-31d0-11eb-9f14-57b78d5aa3ae.png)


ETL pipeline
--------------------------------
The ETL pipeline (see etl.py) loads the S3 data sources into Spark dataframes, aggregrates and transforms the data into the described schema and writes the data back to S3 in the parquet format.

Instructions
------------------------

Create an AWS IAM role with S3 read and write access.
Enter the IAM's credentials in the dl.cfg configuration file.
Create an S3 bucket (note that the zone eu-central-1 may cause issues) and enter the URL to the bucket in etl.py as the value of output_data.
Run python3 etl.py to process the data and store it on your created S3 bucket.
