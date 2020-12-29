from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, date_trunc, desc, mean, stddev, from_json, col, expr, size, collect_list, udf, from_unixtime, window, to_timestamp, sum, array_distinct, explode
from pyspark.sql.types import StructType, StructField, TimestampType, DateType, DecimalType,  StringType, ShortType, BinaryType, ByteType, MapType, FloatType, NullType, BooleanType, DoubleType, IntegerType, ArrayType, LongType
from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("TikTok Static Test") \
        .master("local[3]") \
        .getOrCreate()

    #logger = Log4j(spark)

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

    #write codes to schedule task?
    #TBD

    #Read raw tdata from tiktok daily batch
    raw_df = spark.read \
        .format("parquet") \
        .schema(schema) \
        .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/test_partitioned_data.parquet/year=2015/")


    #Parse out raw table
    filtered_df = raw_df.selectExpr("authorInfos.uniqueId",
                                      "authorInfos.userId",
                                      "challengeInfoList.challengeId",
                                      "challengeInfoList.challengeName",
                                      "challengeInfoList.isCommerce",
                                      "itemInfos.commentCount",
                                      "itemInfos.createTime",
                                      "itemInfos.diggCount",
                                     "itemInfos.id",
                                     "itemInfos.isActivityItem",
                                     "itemInfos.shareCount",
                                     "itemInfos.text",
                                     "musicInfos.authorName",
                                     "musicInfos.musicId",
                                     "musicInfos.musicName")

    #Create engagement metrics
    filtered_df = filtered_df \
        .withColumn("createTime", to_timestamp(from_unixtime(col("createTime").cast(IntegerType()),"yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("date", date_trunc("day", "createTime")) \
        .withColumn("engagementCount", expr("commentCount + diggCount + shareCount")) \
        .withColumnRenamed("authorName", "musicianName")

    #Challenge Table
    challenge_df = filtered_df \
        .groupBy(col("date"), col("challengeName")) \
        .agg(sum(col("engagementCount")).alias("TotalEngagement")) \
        .orderBy(col("TotalEngagement").desc())

    #Author Table
    author_df = filtered_df \
        .groupBy(col("date"), col("userId")) \
        .agg(sum(col("engagementCount")).alias("TotalEngagement")) \
        .orderBy(col("TotalEngagement").desc())

    #Music Table
    music_df = filtered_df \
        .groupBy(col("date"), col("musicId")) \
        .agg(sum(col("engagementCount")).alias("TotalEngagement")) \
        .orderBy(col("TotalEngagement").desc())

    # #Musician Table
    musician_df = filtered_df \
        .groupBy(col("date"), col("musicianName")) \
        .agg(sum(col("engagementCount")).alias("TotalEngagement")) \
        .orderBy(col("TotalEngagement").desc())

    #create wordcount table
    wordcount_df = filtered_df \
        .select(col("date"), col("id"), explode(array_distinct(expr("split(text, ' ')"))).alias("words")) \
        .groupBy(col("date"), col("words")) \
        .agg(collect_list(col("id")).alias("ids")) \
        .withColumn("TotalMentions", size(col("ids")))

    #Save Wordcount table to parquet
    wordcount_df \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date"))) \
        .write.partitionBy("year", "month", "day") \
        .parquet("wc_partition.parquet",mode="append")
