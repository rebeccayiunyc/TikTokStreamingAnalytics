from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, mean, stddev, col, max,min

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Rolling Stats test") \
        .master("local[3]") \
        .getOrCreate()

    #Write codes to schedule the following script at 00:00:00 on every new date
    # now_date =
    now_month = 10
    start_month = str(now_month - 1)
    window_length = 0.5
    stream_window_num = 24 / window_length

    # Read raw tdata from tiktok daily batch
    raw_df = spark.read \
        .format("parquet") \
        .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/wc_partition.parquet/year=2015/" + "month=" + start_month + "/")

    stats_df = raw_df \
        .groupBy("words") \
        .agg(max(col("date")).alias("latest_occur_date"), mean(col("TotalMentions") / stream_window_num).alias("avg_mentions"), stddev(col("TotalMentions") / stream_window_num).alias("std_mentions")) \
        .fillna(0) \
        .orderBy(col("avg_mentions").desc())

    # stats_df = sorted_df \
    #         .withColumn("year", year(col("createTime"))) \
    #         .withColumn("month", month(col("createTime"))) \
    #         .withColumn("day", dayofmonth(col("createTime"))) \
    #         .drop("createTime") \
    #         .write.partitionBy("year", "month", "day") \
    #         .parquet("test_partitioned_data.parquet",mode="append")
    #
    # stats_df.show()
