#from pyspark.sql.functions import sum, mean, stddev, col, max, year, month, dayofmonth, min
#from pyspark.sql import Window
import datetime
import json


def string_to_json(string_value):
    """
    Converts json-like string to a dictionary, then converts to json. Solves inconsistency
    between forms of Boolean data
    :param string_value:
    :return: json data
    """
    #b_to_string = byte_value.decode("utf-8")
    string_to_dict = eval(string_value)
    dict_to_json = json.dumps(string_to_dict)
    return dict_to_json

def subscribe_kafka_topic(ss, topic, offset_mode):
    """
    Reads Kafka data to a streaming dataframe
    :param ss: a Spark session
    :param topic: Kafka topic
    :param offset_mode: startingOffsets option
    :return: Spark streaming dataframe
    """
    df = ss.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", offset_mode) \
    .load()
    return df

def read_static_df(ss, topic):
    """
    Reads Kafka data to a static dataframe
    :param ss: Spark session
    :param topic: kafka topic
    :return:
    """
    df = ss.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()
    return df

def sink_streaming(df,epoch_id):
    """
    Sinks streaming data to an S3 bucket in parquet
    :param df: streaming dataframe
    :param epoch_id:
    """
    now = datetime.datetime.now()
    now_date = now.strftime("%Y-%m-%d")

    df.write \
    .mode("append") \
    .format("parquet") \
    .save("s3a://tiktokstreamingproject/rawkafka/" + now_date +"/")

def sink_word_count(df,epoch_id):
    """
    Sinks wordcount and outlier results to an S3 bucket
    :param df: streaming dartaframe
    :param epoch_id:
    """
    now = datetime.datetime.now()
    now_date = now.strftime("%Y-%m-%d")

    df.write \
    .mode('append') \
    .format("parquet") \
    .save('s3a://tiktokstreamingproject/wordcount/' + now_date +'/')

def writestream_console(df, mode, trigger_time):
    """
    Writes streaming dataframe to console for display
    :param df: Spark streaming dataframe
    :param mode: output mode
    :param trigger_time: string, window length. e.g.: "1 minute"
    """
    written_query = df.writeStream \
        .format("console") \
        .outputMode(mode) \
        .trigger(processingTime=trigger_time) \
        .start()
    return written_query

def writestream_kafka(df, topic, mode, checkpoint_dir, trigger_time):
    """
    Writes streaming data to a new kafka topic
    :param df: streaming dataframe
    :param topic: Kafka topic name
    :param mode: Output mode
    :param checkpoint_dir: checkpoint directory
    :param trigger_time: String, trigger time
    """
    written_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode(mode) \
        .trigger(processingTime=trigger_time) \
        .start()
    return written_query

# def get_avg_std(df):
#     #w = (Window.partitionBy("words").orderBy("end").rowsBetween(-2, 1))
#     #stats_df = df.withColumn('rolling_average', mean("TotalMentions").over(w))
#     stats_df = df \
#         .groupBy("words") \
#         .agg(max(col("end")).alias("latest_endtime"), mean(col("TotalMentions")).alias("avg_mentions"), stddev(col("TotalMentions")).alias("std_mentions"))
#     return stats_df