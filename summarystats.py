import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum, mean, stddev, col, max,min, year, month, dayofmonth

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Rolling Stats test") \
        .master("local[3]") \
        .getOrCreate()

    #Write codes to schedule the following script at 00:00:00 on every new date
    # now_date =
    #now_year = 2015
    #now_month = 7
    #start_month = str(now_month - 1)
    window_length = 0.25
    stream_window_num = 24 / window_length

    # Read data from wordcount database
    raw_df = spark.read \
        .format("parquet") \
        .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/wordcountsink/2020-12-30")

    #for monthly stats with specificed folder
    # stats_df = raw_df \
    #     .groupBy("words") \
    #     .agg(max(col("date")).alias("latest_occur_date"), mean(col("TotalMentions") / stream_window_num).alias("avg_mentions"), stddev(col("TotalMentions") / stream_window_num).alias("std_mentions")) \
    #     .fillna(0) \
    #     .orderBy(col("avg_mentions").desc())

    #save this to weekly statistic database
    stats_window = Window.partitionBy("words").orderBy("year", "month", "day").rowsBetween(-6, 0)
    rolling_stats  = raw_df.withColumn("day_avg_mentions", mean(col("TotalMentions")).over(stats_window)) \
        .withColumn("day_std_mentions", stddev(col("TotalMentions")).over(stats_window)) \
        .withColumn("avg_mentions",  col("day_avg_mentions") / stream_window_num) \
        .withColumn("std_mentions", col("day_std_mentions") / stream_window_num) \
        .drop("day_avg_mentions", "day_std_mentions", "TotalMentions", "ids") \
        .fillna(0)

    now = datetime.datetime.now()
    now_date = now.strftime("%Y-%m-%d")
    rolling_stats \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save('./stats_df/' + now_date + '/')

    # filtered_stats_df = stats_df.withColumn("maxmonth", max("month").over(stats_window)) \
    #     .where(col("month") == col("maxmonth")) \
    #     .drop("maxmonth")
    #
    # stats_df = raw_df \
    #     .orderBy(col("year").desc(), col("month").desc(), col("day").desc()) \
    #     .groupBy("words", "year", "month", "day") \
    #     .agg(mean(col("TotalMentions") / stream_window_num).alias("avg_mentions"), stddev(col("TotalMentions") / stream_window_num).alias("std_mentions")) \
    #     .fillna(0) \

    # stats_write_df = stats_df \
    #         .withColumn("year", year(col("latest_occur_date"))) \
    #         .withColumn("month", month(col("latest_occur_date"))) \
    #         .drop("latest_occur_date") \
    #         .write.partitionBy("year", "month") \
    #         .parquet("stats_partitioned_data.parquet",mode="append")

    # stats_write_df = stats_df \
    #         .write.partitionBy("year", "month") \
    #         .parquet("stats_partitioned_data.parquet",mode="append")
