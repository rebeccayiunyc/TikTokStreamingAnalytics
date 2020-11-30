from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, size, collect_list, udf, from_unixtime, window, to_timestamp, sum, array_distinct, explode
from pyspark.sql.types import StructType, StructField, TimestampType, DateType, DecimalType,  StringType, ShortType, BinaryType, ByteType, MapType, FloatType, NullType, BooleanType, DoubleType, IntegerType, ArrayType, LongType
from lib.logger import Log4j
from udf import string_to_json
import json

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("TikTok Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    logger = Log4j(spark)

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
        ]))
    ])


    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tiktok") \
        .option("startingOffsets", "earliest") \
        .load()


    json_parser_udf = udf(string_to_json, StringType())
    json_df = kafka_df.select(json_parser_udf(col("value").cast("string")).alias("value"))
    json_df = json_df.select(from_json(col("value"), schema).alias("value"))

    filtered_df = json_df.selectExpr("value.authorInfos.uniqueId",
                                      "value.authorInfos.userId",
                                      "value.challengeInfoList.challengeId",
                                      "value.challengeInfoList.challengeName",
                                      "value.challengeInfoList.isCommerce",
                                      "value.itemInfos.commentCount",
                                      "value.itemInfos.createTime",
                                      "value.itemInfos.diggCount",
                                     "value.itemInfos.id",
                                     "value.itemInfos.isActivityItem",
                                     "value.itemInfos.shareCount",
                                     "value.itemInfos.text",
                                     "value.musicInfos.authorName",
                                     "value.musicInfos.musicId",
                                     "value.musicInfos.musicName")

    filtered_df = filtered_df \
        .withColumn("createTime", to_timestamp(from_unixtime(col("createTime").cast(IntegerType()),"yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("engagementCount", expr("commentCount + diggCount + shareCount")) \
        .withColumnRenamed("authorName", "musicianName")


    # #Challenge Table
    # challenge_df = filtered_df \
    #     .groupBy(col("challengeName"),
    #              window(col("createTime"), "10 minute")) \
    #     .agg(sum(col("engagementCount")).alias("TotalEngagement"))
    # challenge_expr = challenge_df.select("window.start", "window.end", "challengeName", "TotalEngagement")

    # challenge_query = challenge_expr.writeStream \
    #     .format("console") \
    #     .outputMode("update")\
    #     .trigger(processingTime="1 minute") \
    #     .start()

    # #Author Table
    # author_df = filtered_df \
    #     .groupBy(col("userId"),
    #              window(col("createTime"), "10 minute")) \
    #     .agg(sum(col("engagementCount")).alias("TotalEngagement"))
    # author_expr = author_df.select("window.start", "window.end", "userId", "TotalEngagement")
    #
    # author_query = author_expr.writeStream \
    #     .format("console") \
    #     .outputMode("update")\
    #     .trigger(processingTime="1 minute") \
    #     .start()
    #
    # #Music Table
    # music_df = filtered_df \
    #     .groupBy(col("musicId"),
    #              window(col("createTime"), "10 minute")) \
    #     .agg(sum(col("engagementCount")).alias("TotalEngagement"))
    # music_expr = music_df.select("window.start", "window.end", "musicId", "TotalEngagement")
    #
    # music_query = music_expr.writeStream \
    #     .format("console") \
    #     .outputMode("update")\
    #     .trigger(processingTime="1 minute") \
    #     .start()
    #
    # #Musician Table
    # musician_df = filtered_df \
    #     .groupBy(col("musicianName"),
    #              window(col("createTime"), "10 minute")) \
    #     .agg(sum(col("engagementCount")).alias("TotalEngagement"))
    # musician_expr = musician_df.select("window.start", "window.end", "musicianName", "TotalEngagement")
    #
    # musician_query = musician_expr.writeStream \
    #     .format("console") \
    #     .outputMode("update")\
    #     .trigger(processingTime="1 minute") \
    #     .start()

    word_id_df = filtered_df \
        .select(col("createTime"),
                col("id"), explode(array_distinct(expr("split(text, ' ')"))).alias("words"))


    wordcount_df = word_id_df \
        .withWatermark("createTime", "15 minute") \
        .groupBy(col("words"),
                 window(col("createTime"), "30 minute", "15 minute")) \
        .agg(collect_list(col("id")).alias("ids")) \
        .withColumn("TotalMentions", size(col("ids")))


    test_query = wordcount_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .start()



    #Final Query would look like this but allows users to subscribe to any one keyword value
    lookup_df = wordcount_df \
        .filter(expr("words = 'Holidays'")) \
        .select(col("window"), col("words"), col("TotalMentions"))

    # #1. Add sliding windows and watermark (checked)
    # #2. Research abnormaly algorithm
    lookup_query = lookup_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .start()

    spark.streams.awaitAnyTermination()


    #
    # #filtered_df.printSchema()
    # filtered_df.show()
    # value_writer_query = value_df.writeStream \
    #     .format("console") \
    #     .queryName("Value Writer") \
    #     .outputMode("append") \
    #     .trigger(processingTime="1 minute") \
    #     .start()
        # .option("path", "output") \
        # .option("checkpointLocation", "chk-point-dir") \

    #
    # logger.info("Listening to Kafka")
    #value_writer_query.awaitTermination()
