from pyspark.sql.functions import sum, mean, stddev, col, max, year, month, dayofmonth, min
from pyspark.sql import Window

import json

def string_to_json(string_value):
    #b_to_string = byte_value.decode("utf-8")
    string_to_dict = eval(string_value)
    dict_to_json = json.dumps(string_to_dict)
    return dict_to_json

def anomalyDetector():
    #insert function here
    #abs(value - mean) >= 2.5 * standard deviation
    pass

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

def read_static_df(ss, topic):
    df = ss.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()
    return df

def sink_batch_time(df,epoch_id):
    # Transform and write batchDF
    df.select(col("year"), col("month"), col("day")) \
    .write \
    .mode('overwrite') \
    .parquet('sinkbatchdate/')

def get_avg_std(df):
    #w = (Window.partitionBy("words").orderBy("end").rowsBetween(-2, 1))
    #stats_df = df.withColumn('rolling_average', mean("TotalMentions").over(w))
    stats_df = df \
        .groupBy("words") \
        .agg(max(col("end")).alias("latest_endtime"), mean(col("TotalMentions")).alias("avg_mentions"), stddev(col("TotalMentions")).alias("std_mentions"))
    return stats_df

def get_early_stream_date(df):
    early_stream_date = df \
        .groupBy() \
        .agg(min(col("start")).alias("earliest_date")) \
            .withColumn("year", year(col("earliest_date"))) \
            .withColumn("month", month(col("earliest_date"))) \
            .withColumn("day", dayofmonth(col("earliest_date"))) \
            .drop("earliest_date")
    return early_stream_date

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

# stats_query = stats_expr.writeStream \
#     .format("console") \
#     .outputMode("update") \
#     .start()


# stats_expr = stats_df.select("words", "sum(TotalMentions)")