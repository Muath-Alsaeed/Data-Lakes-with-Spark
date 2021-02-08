import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()


config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['key']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['secret']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
       """
    Processes all JSON  song data files frominput_data and stores them in parquet format in       
    the output folder.
    :param spark: spark session
    :param input_data: input data path
    :param output_data: output data path
    """
    
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    
    df = spark.read.json(song_data)

 
    song_data = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
            .dropDuplicates()
    
   
    song_data.write.parquet(path = output_data,'data/songs_table' ,partitionBy =                                          ['year','artist_id'],mode='overwrite')
           
    
    artists_table = df.selectExpr('artist_id as id', 'artist_name', 'artist_location',                                       'artist_latitude', 'artist_longitude') \
                    .dropDuplicates()


    
    
    artists_table.write.parquet(path=output_data,'data/artists_table',mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
       """
    Description:
    Processes all JSON log_data files from  input_data and stores them in parquet format in       
    the output folder.
    :param spark: a spark session 
    :param input_data: input data path
    :param output_data: output data path
    """

   
    log_data = input_data + 'log_data/*.json'

   
    df2 = spark.read.json(log_data)
    
   
    df2 = df2.filter("page = 'NextSong'")

      
    users_table = df2.select('userId','firstName','lastName','gender','level').dropDuplicates()

    
    users_table.write.parquet(path = output_data,'data/users_table',mode = 'overwrite')

   
    get_timestamp = udf(lambda x: int(int(x)/1000),IntegerType())
    df2 = df2.withColumn('TimeSatamp', get_timestamp(df2.ts))
    
    
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df2 = df2.withColumn('start_time', get_datetime(df2.ts))
    
    
    time_table =  df2.select('start_time')\
                           .withColumn('hour', F.hour('start_time')) \
                           .withColumn('day', F.dayofmonth('start_time')) \
                           .withColumn('week', F.weekofyear('start_time')) \
                           .withColumn('month', F.month('start_time')) \
                           .withColumn('year', F.year('start_time')) \
                           .withColumn('weekday', F.dayofweek('start_time')) \
                           .dropDuplicates()
    

    time_table.write.parquet(path=output_data,'data/time_table',partitionBy = ['year','month'],mode = 'overwrite')

    song_schema = StructType([
        StructField("id", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("start_time", DateType()),
        StructField("location", StringType()),
        StructField("userAgent", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),,
        StructField("duration", DoubleType()),
        StructField("level", StringType()),
        StructField("sessionId", IntegerType()),
        StructField("year", IntegerType()) ] )
        
        
    
    df = spark.read.json("data/song_data/A/B/C/*.json",schema=song_schema)
    song_df = df.select('song_id','artist_id','artist_name')
    
    df2 = spark.read.json(log_data,schema=song_schema)

    
    log_df = df2.selectExpr('start_time','userId','level','sessionId'
                            ,'location','userAgent','artist as artist_name')
    
    songplays_table = log_df.join(song_df,on='artist_name',how='inner')
    
   
    songplays_table = songplays_table.withColumn('id',F.monotonically_increasing_id())\
    .withColumn('month',F.month('start_time'))\
    .withColumn('year',F.year('start_time'))
    
    songplays_table.write.parquet(path=output_data , 'songplays_table'
                                  ,partitionBy=['year','month']
                                  ,mode='overwrite') 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
