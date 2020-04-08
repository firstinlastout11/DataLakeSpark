import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function serves to create a spark sesion with the given version of hadoop-aws
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function takes the input song data from a S3 and processes the data into each song_table and artsits_table.
    Finally, it stores the created tables as parquet files in the output_data folder at the output S3 bucket
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration']).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+"songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id',col('artist_name').alias('name'),col('artist_location').alias('location'),col('artist_latitude').alias('latitude'),col('artist_longitude').alias('longitude')).distinct()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"artists")


def process_log_data(spark, input_data, output_data):
    """
    This functino takes the input log data from a S3 bucket and prcoesses the data into users table, time table and songplays table.
    Finally, it stores the created tables as parquet files in the output_data folder at the output S3 bucket.
    """
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'), col('lastName').alias('last_name'), 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+"users")

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", F.from_unixtime((df.ts/1000), format = 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', F.to_date(F.from_unixtime((df.ts/1000), format = 'yyyy-MM-dd HH:mm:ss').cast('timestamp')))
    
    df = df.withColumn('year', F.year('datetime'))\
        .withColumn('month', F.month('datetime'))

    # extract columns to create time table
    time_table = df.withColumn('hour', F.hour('start_time'))\
            .withColumn('day', F.dayofmonth('datetime'))\
            .withColumn('week', F.weekofyear('datetime'))\
            .withColumn('weekday', F.date_format('datetime','EEEE'))\
            .select(['start_time','hour','day','week','month','year','weekday'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'time')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.withColumn('songplay_id', F.monotonically_increasing_id())\
        .join(song_df, df.song == song_df.title, how='left').select('songplay_id','start_time',col('userId').alias('user_id'),\
            'level','song_id','artist_id',col('sessionId').alias('session_id'),\
            'location', col('userAgent').alias('user_agent') ,df.year,'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'songplay')
 

def main():
    """
    The main function to run the functions above.
    The output_data directory needs to be specified as the correct S3 bucket of the user.
    """
    spark = create_spark_session()
    #spark.conf.set("spark.executor.memory", "15g")
    input_data = "s3a://udacity-dend/"
    #input_data = "data/"
    #output_data = "data/output_data/"
    output_data = "s3a://spark-sparkify/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
