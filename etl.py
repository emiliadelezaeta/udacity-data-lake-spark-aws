import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F

from pyspark.sql.types import IntegerType, BooleanType, DateType, DoubleType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    print(spark)

    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/*/*.json'

    # read song data file
    df_song_data = spark.read.json(song_data)

    # casting some columns
    df_song_data = df_song_data.withColumn("year", df_song_data.year.cast('integer'))

    # extract columns to create songs table
    songs_table = df_song_data.select(df_song_data.song_id
                                      , df_song_data.title
                                      , df_song_data.artist_id
                                      , df_song_data.year
                                      , df_song_data.duration)

    # removing duplicated records by song_id
    songs_table = songs_table.dropDuplicates(['song_id'])

    #print(songs_table.printSchema())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df_song_data.select(df_song_data.artist_id
                                        , df_song_data.artist_name
                                        , df_song_data.artist_location
                                        , df_song_data.artist_latitude
                                        , df_song_data.artist_longitude)

    # removing duplicated records by artist_id
    artists_table = artists_table.dropDuplicates(['artist_id'])

    #print(artists_table.printSchema())

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')

    return df_song_data


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df_log_data = spark.read.json(log_data)

    # filter by actions for song plays
    df_log_data = df_log_data[df_log_data.page == 'NextSong']

    # casting some columns
    df_log_data = df_log_data.withColumn("ts", F.to_timestamp(df_log_data.ts.cast('bigint') / 1000))
    df_log_data = df_log_data.withColumn("userId", df_log_data.userId.cast('integer'))
    df_log_data = df_log_data.withColumn("sessionId", df_log_data.userId.cast('integer'))

    # extract columns for users table    
    users_table = df_log_data.select(df_log_data.userId
                                     , df_log_data.firstName
                                     , df_log_data.lastName
                                     , df_log_data.level
                                     , df_log_data.gender)

    # removing duplicated records by userId
    users_table = users_table.dropDuplicates(['userId'])

    # print(users_table.printSchema())

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # extract columns to create time table
    time_table = df_log_data.select(df_log_data.ts
                                    , F.hour("ts").alias('hour')
                                    , F.dayofmonth("ts").alias('day')
                                    , F.weekofyear("ts").alias('week')
                                    , F.month("ts").alias('month')
                                    , F.year("ts").alias('year')
                                    , F.dayofweek("ts").alias('weekday'))

    #print(time_table.printSchema())

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')

    return df_log_data


def create_songsplay_table(song_data, log_data, output_data):
    # read in song data to use for songplays table
    song_df = song_data.select(song_data.song_id
                               , song_data.artist_id
                               , song_data.title
                               , song_data.artist_name)

    # read in log data to use for songplays table
    event_df = log_data.select(log_data.ts
                               , F.year("ts").alias('year')
                               , F.month("ts").alias('month')
                               , log_data.artist
                               , log_data.song
                               , log_data.userId
                               , log_data.level
                               , log_data.sessionId
                               , log_data.location
                               , log_data.userAgent)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = event_df.join(song_df,
                                    (event_df.artist == song_df.artist_name) & (event_df.song == song_df.title))

    # selecting only the columns necessary to create the fact table
    songplays_table = songplays_table.select(songplays_table.ts
                                             , songplays_table.year
                                             , songplays_table.month
                                             , songplays_table.userId
                                             , songplays_table.level
                                             , songplays_table.song_id
                                             , songplays_table.artist_id
                                             , songplays_table.sessionId
                                             , songplays_table.location
                                             , songplays_table.userAgent)

    # print(songplays_table.printSchema())
    # print(songplays_table.count())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songsplay')


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # read credentials previously created with AWS CLI
    os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]

    # data source
    input_data = 's3a://udacity-dend/'

    # output S3 bucket previously created with AWS CLI
    output_data = 's3a://udacity-datalake-sparkify-edls/'

    spark = create_spark_session()

    '''
        process_song_data --> 
            - function that reads data from songs input file and creates the dimensions tables artists and songs
            - saves as parquet files the dimensions tables in the output S3 butcket
            - it returns a pyspark dataframe with the songs data 
            
        process_log_data --> 
            - function that reads data from logs input file and creates the dimensions tables users and time
            - saves as parquet files the dimensions tables in the output S3 butcket
            - it returns a pyspark dataframe with the logs data
            
        create_songsplay_table --> 
            - joins previous pyspark dataframes coming from the functions 'process_song_data' and'process_log_data'
            and creates the fact table songplays_table
            - save as parquet files the fact table in the output S3 butcket    
    '''

    df_song = process_song_data(spark, input_data, output_data)
    df_log = process_log_data(spark, input_data, output_data)
    create_songsplay_table(df_song, df_log, output_data)


if __name__ == "__main__":
    main()
