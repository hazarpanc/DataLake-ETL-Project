import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType, DateType
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session.
    
    Returns:
            Spark session object
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Extracts song data from json files on AWS S3. Writes extracted data to AWS S3 data lake bucket.
    
    Arguments:
            spark {object}:        The entry point to programming Spark with the Dataset and DataFrame API.
            input_data {string}:   S3 bucket where Sparkify's event data is stored 
            output_data {string}:  S3 bucket to store extracted parquet data file
    Returns:
            No return values 
    """

    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*'
    
    # define the schema for songs table
    songSchema = StructType([
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("song_id",StringType()),
        StructField("title",StringType()),
        StructField("year",IntegerType()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema).drop_duplicates()

    # create temporary view
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("SELECT song_id, title, artist_id, year, duration FROM songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(path=output_data+"songs/songs.parquet", partitionBy=["year","artist_id"], mode="overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs")
    
    # write artists table to parquet files
    artists_table.write.parquet(path=output_data+"artists/artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """Extracts log data from json files on AWS S3. Runs transformations and extracts user, time and user activity data. Finally, loads the data to AWS S3 data lake bucket.
    
    Arguments:
            spark {object}:        The entry point to programming Spark with the Dataset and DataFrame API.
            input_data {string}:   S3 bucket where Sparkify's event data is stored 
            output_data {string}:  S3 bucket to store extracted parquet data files
    Returns:
            No return values
    
    """
  
    # get filepath to log data file
    log_data = log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data).drop_duplicates()
    
    # create logs view
    df.createOrReplaceTempView("logs")
    
    # filter by actions for song plays
    df = spark.sql("SELECT * FROM logs WHERE page='NextSong'")

    # extract columns for users table    
    users_table = spark.sql("SELECT userId, firstName, lastName, gender, level FROM logs WHERE page='NextSong'")
    users_table = users_table.drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(path=output_data+"users/users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    to_timestamp_udf = udf(lambda ts_string: \
                           datetime.fromtimestamp(ts_string /1000.0) \
                           ,TimestampType())
    spark.udf.register("to_timestamp", to_timestamp_udf)
    
    # create datetime column from original timestamp column
    # extract columns to create time table
    time_table = spark.sql(
    '''
    SELECT 
      ts, 
      hour(to_timestamp(ts)) AS hour,
      day(to_timestamp(ts)) AS day,
      weekofyear(to_timestamp(ts)) AS week,
      month(to_timestamp(ts)) AS month,
      year(to_timestamp(ts)) AS year,
      weekday(to_timestamp(ts)) AS weekday
    FROM logs
    '''
    )
    time_table = time_table.withColumn('start_time', \
                            to_timestamp_udf('ts').cast(TimestampType()))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path=output_data+"time/time.parquet",\
                             mode="overwrite", \
                             partitionBy=["year","month"])

    # define the schema for songs table
    songSchema = StructType([
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("song_id",StringType()),
        StructField("title",StringType()),
        StructField("year",IntegerType()),
    ])

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*'
    song_df = spark.read.json(song_data, schema=songSchema).drop_duplicates()
    song_df.createOrReplaceTempView("songs")
    songs_table = spark.sql("SELECT song_id, title, artist_id, year, duration FROM songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
    SELECT 
      to_timestamp(l.ts) AS start_time, 
      l.userId, 
      l.level, 
      s.song_id, 
      s.artist_id, 
      l.sessionId, 
      l.location, 
      l.userAgent,
      year(to_timestamp(l.ts)) AS year,
      month(to_timestamp(l.ts)) AS month
    FROM 
      logs l 
      INNER JOIN songs s ON l.song = s.title AND l.artist = s.artist_name 
    ORDER BY 
      l.ts 
    ''')
    songplays_table = songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(path=output_data+"songplays/songplays.parquet", mode="overwrite", partitionBy=["year","month"])


def main():
    """
    Run the ETL process.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://data-lake-project-hp/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
