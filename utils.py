#from pyspark.sql.functions import sum, mean, stddev, col, max, year, month, dayofmonth, min
#from pyspark.sql import Window
import datetime
import json


def string_to_json(string_value):
    #b_to_string = byte_value.decode("utf-8")
    string_to_dict = eval(string_value)
    dict_to_json = json.dumps(string_to_dict)
    return dict_to_json

def subscribe_kafka_topic(ss, topic):
    df = ss.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()
    return df

def read_static_df(ss, topic):
    df = ss.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()
    return df

def sink_streaming(df,epoch_id):
    now = datetime.datetime.now()
    now_date = now.strftime("%Y-%m-%d")

    df.write \
    .mode("append") \
    .format("parquet") \
    .save("s3a://tiktokstreamingproject/rawkafka/" + now_date +"/")

def sink_word_count(df,epoch_id):
    now = datetime.datetime.now()
    now_date = now.strftime("%Y-%m-%d")

    df.write \
    .mode('append') \
    .format("parquet") \
    .save('s3a://tiktokstreamingproject/wordcount/' + now_date +'/')

# def get_avg_std(df):
#     #w = (Window.partitionBy("words").orderBy("end").rowsBetween(-2, 1))
#     #stats_df = df.withColumn('rolling_average', mean("TotalMentions").over(w))
#     stats_df = df \
#         .groupBy("words") \
#         .agg(max(col("end")).alias("latest_endtime"), mean(col("TotalMentions")).alias("avg_mentions"), stddev(col("TotalMentions")).alias("std_mentions"))
#     return stats_df

def writestream_console(df, mode):
    written_query = df.writeStream \
        .format("console") \
        .outputMode(mode) \
        .trigger(processingTime="1 minute") \
        .start()
    return written_query

def writestream_kafka(df, topic, mode, checkpoint_dir):
    written_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode(mode) \
        .trigger(processingTime="1 minute") \
        .start()
    return written_query
