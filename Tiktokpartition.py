from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, when, mean, stddev, from_json, col, expr, size, collect_list, udf, from_unixtime, window, to_timestamp, sum, array_distinct, explode
from pyspark.sql.types import StructType, StructField, TimestampType, DateType, DecimalType,  StringType, ShortType, BinaryType, ByteType, MapType, FloatType, NullType, BooleanType, DoubleType, IntegerType, ArrayType, LongType
from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("TikTok Write to Parquet") \
        .master("local[3]") \
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

    #Read batch data from Ndjson
    raw_df = spark.read.json("./datagen/data/tiktok_5000000_sample.ndjson", schema)

    #Sort dataframe by date
    sorted_df = raw_df.dropDuplicates() \
            .orderBy("itemInfos.createTime") \
        .withColumn("createTime", to_timestamp(from_unixtime(col("itemInfos.createTime").cast(IntegerType()),"yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss"))

    #write dataframe by date partition
    bydate_df = sorted_df \
            .withColumn("year", year(col("createTime"))) \
            .withColumn("month", month(col("createTime"))) \
            .withColumn("day", dayofmonth(col("createTime"))) \
            .drop("createTime") \
            .write.partitionBy("year", "month", "day") \
            .parquet("test_partitioned_data.parquet",mode="append")
