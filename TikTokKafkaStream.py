from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, when, mean, stddev, from_json, col, expr, size, collect_list, udf, from_unixtime, window, to_timestamp, sum, array_distinct, explode
from pyspark.sql.types import StructType, StructField, TimestampType, DateType, DecimalType,  StringType, ShortType, BinaryType, ByteType, MapType, FloatType, NullType, BooleanType, DoubleType, IntegerType, ArrayType, LongType
from lib.logger import Log4j
from utils import subscribe_kafka_topic, get_avg_std, get_max_date, writestream_kafka, writestream_console, string_to_json, read_static_df, sink_batch_time

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
    # schema = StructType([
    #     StructField("authorInfos", StructType([
    #         StructField("covers", ArrayType(StringType())),
    #         StructField("coversLarger", ArrayType(StringType())),
    #         StructField("coversMedium", ArrayType(StringType())),
    #         StructField("nickName", StringType()),
    #         StructField("secUid", StringType()),
    #         StructField("signature", StringType()),
    #         StructField("uniqueId", StringType()),
    #         StructField("userId", StringType())
    #         ])),
    #     StructField("challengeInfoList", ArrayType(StructType([
    #         StructField("challengeId", StringType()),
    #         StructField("challengeName", StringType()),
    #         StructField("covers", ArrayType(StringType())),
    #         StructField("coversLarger", ArrayType(StringType())),
    #         StructField("coversMedium", ArrayType(StringType())),
    #         StructField("isCommerce", BooleanType()),
    #         StructField("text", StringType())
    #     ]))),
    #     StructField("itemInfos", StructType([
    #         StructField("authorId", StringType()),
    #         StructField("commentCount", LongType()),
    #         StructField("covers", ArrayType(StringType())),
    #         StructField("coversDynamic", ArrayType(StringType())),
    #         StructField("coversOrigin", ArrayType(StringType())),
    #         StructField("createTime", StringType()),
    #         StructField("diggCount", LongType()),
    #         StructField("id", StringType()),
    #         StructField("isActivityItem", BooleanType()),
    #         StructField("musicId", StringType()),
    #         StructField("shareCount", LongType()),
    #         StructField("text", StringType()),
    #         StructField("video", StructType([
    #             StructField("url", ArrayType(StringType())),
    #             StructField("videoMeta", StructType([
    #                 StructField("Duration", LongType()),
    #                 StructField("height", LongType()),
    #                 StructField("ratio", LongType()),
    #                 StructField("width", LongType())
    #             ]))
    #         ]))
    #     ])),
    #     StructField("musicInfos", StructType([
    #         StructField("authorName", StringType()),
    #         StructField("covers", ArrayType(StringType())),
    #         StructField("coversLarger", ArrayType(StringType())),
    #         StructField("coversMedium", ArrayType(StringType())),
    #         StructField("musicId", StringType()),
    #         StructField("musicName", StringType()),
    #         StructField("original", StringType()),
    #         StructField("playUrl", ArrayType(StringType()))
    #     ]))
    # ])
    #
    # #Read raw data from tiktok
    # kafka_df = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "tiktok") \
    #     .option("startingOffsets", "earliest") \
    #     .load()
    #
    # #convert raw kafka message to a dataframe
    # json_parser_udf = udf(string_to_json, StringType())
    # json_df = kafka_df.select(json_parser_udf(col("value").cast("string")).alias("value"))
    # json_df = json_df.select(from_json(col("value"), schema).alias("value"))
    #
    # filtered_df = json_df.selectExpr("value.authorInfos.uniqueId",
    #                                   "value.authorInfos.userId",
    #                                   "value.challengeInfoList.challengeId",
    #                                   "value.challengeInfoList.challengeName",
    #                                   "value.challengeInfoList.isCommerce",
    #                                   "value.itemInfos.commentCount",
    #                                   "value.itemInfos.createTime",
    #                                   "value.itemInfos.diggCount",
    #                                  "value.itemInfos.id",
    #                                  "value.itemInfos.isActivityItem",
    #                                  "value.itemInfos.shareCount",
    #                                  "value.itemInfos.text",
    #                                  "value.musicInfos.authorName",
    #                                  "value.musicInfos.musicId",
    #                                  "value.musicInfos.musicName")
    #
    # #Create aggregate windowed table with engagement metrics
    # filtered_df = filtered_df \
    #     .withColumn("createTime", to_timestamp(from_unixtime(col("createTime").cast(IntegerType()),"yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
    #     .withColumn("engagementCount", expr("commentCount + diggCount + shareCount")) \
    #     .withColumnRenamed("authorName", "musicianName")
    #
    # #create word-postId table
    # word_id_df = filtered_df \
    #     .select(col("createTime"),
    #             col("id"), explode(array_distinct(expr("split(text, ' ')"))).alias("words"))
    #
    # #Aggregate word-postId to get wordcount dataframe
    # wordcount_df = word_id_df \
    #     .withWatermark("createTime", "15 minute") \
    #     .groupBy(col("words"),
    #              window(col("createTime"), "30 minute", "15 minute")) \
    #     .agg(collect_list(col("id")).alias("ids")) \
    #     .withColumn("TotalMentions", size(col("ids")))

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
    wc_df = subscribe_kafka_topic(spark, "tiktok_wc")
    wc_json_df = wc_df.select(col("key").cast("string").alias("key"),
                           col("value").cast("string").alias("value"))

    wc_schema = StructType([
        StructField("window", StructType([
            StructField("start", TimestampType()),
            StructField("end", TimestampType())])),
        StructField("ids", ArrayType(StringType())),
        StructField("TotalMentions", IntegerType())])

    wc_json_df = wc_json_df.select(col("key"),
                                   from_json(col("value"), wc_schema).alias("value"))

    wc_flattened_df = wc_json_df.selectExpr("key as words",
                                            "value.window.start",
                                            "value.window.end",
                                            "value.ids",
                                            "value.TotalMentions") \
        .withWatermark("end", "15 minute")

    #writestream_console(wc_flattened_df, "update")

    #Read average and Standard deviation statistics from stats file
    # early_date = get_max_date(wc_flattened_df)
    # early_date.writeStream.foreachBatch(sink_batch_time).outputMode("complete").start()
    #
    stream_date = spark.read \
        .format("parquet") \
        .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/sinkbatchdate/part-00000-734467b8-8fa6-4fc7-a193-b6b2b8d041a4-c000.snappy.parquet")
    stream_month = stream_date.collect()[0]['month']
    print(stream_month)
    # print(stream_date['month'])
    # now_yr = wc_flattened_df.groupBy("words") \
    #     .agg(max(col("end")).alias("latest_endtime"))
    #writestream_console(early_date, "update")

    # now_month = month(max(wc_flattened_df.start))
    # now_day = dayofmonth(max(wc_flattened_df.start))
    # #
    # wc_stats_kafka_df = wc_stats.selectExpr("words as key",
    #                                           """to_json(named_struct(
    #                                           'latest_endtime', latest_endtime,
    #                                           'avg_mentions', avg_mentions,
    #                                           'std_mentions', std_mentions)) as value
    #                                           """)
    #
    # #wc_query = writestream_kafka(wc_stats_kafka_df, "tiktok_stats", "update", "chk-point-dir-1")
    #
    # #Read Wordcount Stats from Kafka
    # stats_df = subscribe_kafka_topic(spark, "tiktok_stats")
    # stats_json_df = stats_df.select(col("key").cast("string").alias("key"),
    #                           col("value").cast("string").alias("value"))
    #
    # stats_schema = StructType([
    #     StructField("latest_endtime", TimestampType()),
    #     StructField("avg_mentions", FloatType()),
    #     StructField("std_mentions", FloatType())])
    #
    # stats_json_df = stats_json_df.select(col("key"),
    #                                from_json(col("value"), stats_schema).alias("value"))
    #
    # stats_flattened_df = stats_json_df \
    #     .selectExpr("key as word",
    #                 "value.latest_endtime",
    #                 "value.avg_mentions",
    #                 "value.std_mentions") \
    #     .withWatermark("latest_endtime", "15 minute")
    #
    # stats_flatten_query = writestream_console(stats_flattened_df, "update")
    #
    # #Joining WordCount and WordCount Stats Stream
    # #join_expr = """"word=words AND latest_endtime >= end_time"""
    # joined_df = wc_flattened_df.join(
    #     stats_flattened_df,
    #     expr("word=words AND latest_endtime BETWEEN end - interval 30 minutes and end"), "leftOuter") \
    #     .withColumn("Outlier", when(col("TotalMentions") >= col("avg_mentions") + 2.5 * col("std_mentions"), 1) \
    #                 .otherwise(0))
    # joined_query = writestream_console(joined_df, "append")
    #
    # # lookup_query = lookup_df.writeStream \
    # #     .format("console") \
    # #     .outputMode("complete") \
    # #     .trigger(processingTime="1 minute") \
    # #     .start()
    # # #

    spark.streams.awaitAnyTermination()
    #
    # logger.info("Listening to Kafka")