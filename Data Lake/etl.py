import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''build spark interface'''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    reads song data from S3, processes that data using Spark to build tables, and writes them back to S3

    Parameters: 
    spark      : spark interface
    input_data : S3 input bucket
    output_data: S3 output bucket
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    songs_table = songs_table.dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite")\
               .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", "artist_name as name", "artist_location as location", \
                                   "artist_latitude as latitude", "artist_longitude as longitude"])
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
               .parquet(os.path.join(output_data, 'artists'))

def process_log_data(spark, input_data, output_data):
    """ 
    reads log data from S3, processes that data using Spark to build tables, and writes them back to S3

    Parameters: 
    spark      : spark interface
    input_data : S3 input bucket
    output_data: S3 output bucket
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(["userid as user_id", 'firstName as first_name', 'lastName as last_name', 'gender', 'level'])
    users_table = users_table.dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data,'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimstamp(x/1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000), DateType())
    df = df.withColumn('start_time', get_datetime(df.ts))
    
    # filter None in df
    df = df.where(df.start_time.isNotNull())
    
    # extract columns to create time table
    time_table = df.select('start_time', hour(df.start_time).alias('hour'), dayofmonth(df.start_time).alias('day'),\
                                weekofyear(df.start_time).alias('week'), month(df.start_time).alias('month'),\
                                year(df.start_time).alias('year'),dayofweek(df.start_time).alias('weekday'))\
                                .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data,'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.artist==song_df.artist_name) & (df.song==song_df.title) & (df.length==song_df.duration))\
                      .select(col('start_time'), col("userId").alias("user_id"), col("level"), col('song_id'), col('artist_id'),\
                             col('sessionid').alias('session_id'), col('location'), col('useragent').alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data,'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://shan-bucket1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
