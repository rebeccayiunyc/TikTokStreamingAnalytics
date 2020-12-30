from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import max, year, month, dayofmonth, when, mean, stddev, from_json, col, expr, size, collect_list, udf, from_unixtime, window, to_timestamp, sum, array_distinct, explode
from pyspark.sql.types import StructType, StructField, TimestampType, DateType, DecimalType,  StringType, ShortType, BinaryType, ByteType, MapType, FloatType, NullType, BooleanType, DoubleType, IntegerType, ArrayType, LongType
from lib.logger import Log4j
from utils import subscribe_kafka_topic, get_avg_std, get_early_stream_date, writestream_kafka, writestream_console, string_to_json, read_static_df, sink_batch_time

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("TikTok Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    # logger = Log4j(spark)
    #
    schema = StructType([
        StructField("authorInfos", StructType([
            StructField("covers", ArrayType(StringType())),
            StructField("coversLarger", ArrayType(StringType())),
            StructField("coversMedium", ArrayType(StringType())),
            StructField("nickName", StringType()),
            StructField("secUid", StringType()),
            StructField("signature", StringType()),
            StructField("uniqueId", StringType()),
            StructField("userId", StringType())
            ])),
        StructField("challengeInfoList", ArrayType(StructType([
            StructField("challengeId", StringType()),
            StructField("challengeName", StringType()),
            StructField("covers", ArrayType(StringType())),
            StructField("coversLarger", ArrayType(StringType())),
            StructField("coversMedium", ArrayType(StringType())),
            StructField("isCommerce", BooleanType()),
            StructField("text", StringType())
        ]))),
        StructField("itemInfos", StructType([
            StructField("authorId", StringType()),
            StructField("commentCount", LongType()),
            StructField("covers", ArrayType(StringType())),
            StructField("coversDynamic", ArrayType(StringType())),
            StructField("coversOrigin", ArrayType(StringType())),
            StructField("createTime", StringType()),
            StructField("diggCount", LongType()),
            StructField("id", StringType()),
            StructField("isActivityItem", BooleanType()),
            StructField("musicId", StringType()),
            StructField("shareCount", LongType()),
            StructField("text", StringType()),
            StructField("video", StructType([
                StructField("url", ArrayType(StringType())),
                StructField("videoMeta", StructType([
                    StructField("Duration", LongType()),
                    StructField("height", LongType()),
                    StructField("ratio", LongType()),
                    StructField("width", LongType())
                ]))
            ]))
        ])),
        StructField("musicInfos", StructType([
            StructField("authorName", StringType()),
            StructField("covers", ArrayType(StringType())),
            StructField("coversLarger", ArrayType(StringType())),
            StructField("coversMedium", ArrayType(StringType())),
            StructField("musicId", StringType()),
            StructField("musicName", StringType()),
            StructField("original", StringType()),
            StructField("playUrl", ArrayType(StringType()))
        ])),
        StructField("time_stamp", FloatType())
    ])

    #Read raw data from tiktok
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tiktok") \
        .option("startingOffsets", "earliest") \
        .load()

    # #convert raw kafka message to a dataframe
    json_parser_udf = udf(string_to_json, StringType())
    json_df = kafka_df.select(json_parser_udf(col("value").cast("string")).alias("value"))
    json_df = json_df.select(from_json(col("value"), schema).alias("value"))

    filtered_df = json_df.selectExpr("value.authorInfos.uniqueId",
                                      "value.authorInfos.userId",
                                      "value.challengeInfoList.challengeId",
                                      "value.challengeInfoList.challengeName",
                                      "value.challengeInfoList.isCommerce",
                                      "value.itemInfos.commentCount",
                                      "value.itemInfos.diggCount",
                                     "value.itemInfos.id",
                                     "value.itemInfos.isActivityItem",
                                     "value.itemInfos.shareCount",
                                     "value.itemInfos.text",
                                     "value.musicInfos.authorName",
                                     "value.musicInfos.musicId",
                                     "value.musicInfos.musicName",
                                     "value.time_stamp") \
        .withColumn("engagementCount", expr("commentCount + diggCount + shareCount")) \
        .withColumnRenamed("authorName", "musicianName")

    #Write code to sink this to S3 for daily batch processing


    #create wordcount table
    wordcount_df = filtered_df \
        .withColumn("timestamp", to_timestamp(from_unixtime(col("time_stamp").cast(IntegerType()),"yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
        .select(col("timestamp"), col("id"), explode(array_distinct(expr("split(text, ' ')"))).alias("words")) \
        .withWatermark("timestamp", "15 minute") \
        .groupBy(col("words"),
                 window(col("timestamp"), "15 minute", "15 minute")) \
        .agg(collect_list(col("id")).alias("ids")) \
        .withColumn("TotalMentions", size(col("ids")))

    #write codes to sink streaming data to S3/Cassandra
    writestream_console(wordcount_df, "update")

    # #Final Query would look like this but allows users to subscribe to any one keyword value
    # lookup_df = wordcount_df \
    #     .filter(expr("words = 'Holidays'")) \
    #     .select(col("window"), col("words"), col("TotalMentions"))
    #
    # #Prepare wordcount dataframe for Kafka
    # kafka_target_df = wordcount_df.selectExpr("words as key",
    #                                           """to_json(named_struct(
    #                                           'window', window,
    #                                           'ids', ids,
    #                                           'TotalMentions', TotalMentions)) as value
    #                                           """)

    # #Write wordcount dataframe to Kafka
    # wordcount_query = kafka_target_df.writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "tiktok_wc") \
    #     .option("checkpointLocation", "chk-point-dir") \
    #     .outputMode("update") \
    #     .trigger(processingTime="1 minute") \
    #     .start()
    #
    #Read wordcount dataframe from Kafka
    # wc_df = subscribe_kafka_topic(spark, "tiktok_wc")
    # wc_json_df = wc_df.select(col("key").cast("string").alias("key"),
    #                        col("value").cast("string").alias("value"))
    #
    # wc_schema = StructType([
    #     StructField("window", StructType([
    #         StructField("start", TimestampType()),
    #         StructField("end", TimestampType())])),
    #     StructField("ids", ArrayType(StringType())),
    #     StructField("TotalMentions", IntegerType())])
    #
    # wc_json_df = wc_json_df.select(col("key"),
    #                                from_json(col("value"), wc_schema).alias("value"))
    #
    # wc_flattened_df = wc_json_df \
    #     .selectExpr("key as word",
    #     "value.window.start",
    #     "value.window.end",
    #     "value.ids",
    #     "value.TotalMentions") \
    #     .withColumn("end_year", year(col("end"))) \
    #     .withColumn("end_month", month(col("end"))) \
    #     .withWatermark("end", "15 minute")
    #
    # #writestream_console(wc_flattened_df, "update")
    #
    # #Extract Streaming Date
    # # stream_date = get_early_stream_date(wc_flattened_df)
    # # stream_date.writeStream.foreachBatch(sink_batch_time).outputMode("update").start()
    #
    # # stream_date = spark.read \
    # #     .format("parquet") \
    # #     .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/sinkbatchdate/*")
    # # stream_year = stream_date.collect()[0]['year']
    # # stream_month = stream_date.collect()[0]['month']
    # #stream_day = stream_date.collect()[0]['day']
    #
    #
    #Read stats from historic wordcount tables
    # Look for a matching parquet file
    stats_df = spark.read \
        .format("parquet") \
        .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/stats_partitioned_data.parquet/year=" + str(stream_year) + '/month=' + str(stream_month-1) + "/*")

    # ## Load the entire parquet file
    # stats_df = spark.read \
    #     .format("parquet") \
    #     .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/stats_partitioned_data.parquet/") \
    #
    # # stats_df.show()
    # # stats_window = Window.partitionBy("words")
    # # filtered_stats_df = stats_df.withColumn("maxmonth", max("month").over(stats_window)) \
    # #     .where(col("month") == col("maxmonth")) \
    # #     .drop("maxmonth")
    #
    #
    # #Joining WordCount and WordCount Stats Stream
    # #    expr("word=words AND latest_endtime BETWEEN end - interval 30 minutes and end"), "leftOuter") \
    # joined_df = wc_flattened_df.join(
    #     stats_df,
    #     expr("word = words AND end_year = year AND end_month > month"), "leftOuter") \
    #     .fillna(0) \
    #     .withColumn("Outlier", when(col("TotalMentions") >= col("avg_mentions") + 2.5 * col("std_mentions"), 1) \
    #                 .otherwise(0))
    # writestream_console(joined_df, "update")
    #
    #
    # # joined_query = writestream_console(joined_df, "append")
    #
    # # wc_stats_kafka_df = wc_stats.selectExpr("words as key",
    # #                                           """to_json(named_struct(
    # #                                           'latest_endtime', latest_endtime,
    # #                                           'avg_mentions', avg_mentions,
    # #                                           'std_mentions', std_mentions)) as value
    # #                                           """)
    # #
    # # #wc_query = writestream_kafka(wc_stats_kafka_df, "tiktok_stats", "update", "chk-point-dir-1")
    # #
    # # #Read Wordcount Stats from Kafka
    # # stats_df = subscribe_kafka_topic(spark, "tiktok_stats")
    # # stats_json_df = stats_df.select(col("key").cast("string").alias("key"),
    # #                           col("value").cast("string").alias("value"))
    # #
    # # stats_schema = StructType([
    # #     StructField("latest_endtime", TimestampType()),
    # #     StructField("avg_mentions", FloatType()),
    # #     StructField("std_mentions", FloatType())])
    # #
    # # stats_json_df = stats_json_df.select(col("key"),
    # #                                from_json(col("value"), stats_schema).alias("value"))
    # #
    # # stats_flattened_df = stats_json_df \
    # #     .selectExpr("key as word",
    # #                 "value.latest_endtime",
    # #                 "value.avg_mentions",
    # #                 "value.std_mentions") \
    # #     .withWatermark("latest_endtime", "15 minute")
    # #
    #
    #
    # # # lookup_query = lookup_df.writeStream \
    # # #     .format("console") \
    # # #     .outputMode("complete") \
    # # #     .trigger(processingTime="1 minute") \
    # # #     .start()
    # # # #

    spark.streams.awaitAnyTermination()
    #
    # logger.info("Listening to Kafka")