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
    window_length = 0.25
    stream_window_num = 24 / window_length

    # Read data from wordcount database
    raw_df = spark.read \
        .format("parquet") \
        .load("/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/wordcount/")

    #Compute weekly windowed statistic by word
    stats_window = Window.partitionBy("words").orderBy("date").rowsBetween(-6, 0)
    rolling_stats  = raw_df \
        .withColumn("day_avg_mentions", mean(col("TotalMentions")).over(stats_window)) \
        .withColumn("day_std_mentions", stddev(col("TotalMentions")).over(stats_window)) \
        .withColumn("avg_mentions",  col("day_avg_mentions") / stream_window_num) \
        .withColumn("std_mentions", col("day_std_mentions") / stream_window_num) \
        .drop("day_avg_mentions", "day_std_mentions", "TotalMentions", "ids", "window") \
        .fillna(0)

    #save this to weekly statistic database
    now = datetime.datetime.now()
    now_date = now.strftime("%Y-%m-%d")
    rolling_stats \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save('./wc_stats_df/' + now_date + '/')

##------------------ BACKUP content----------------------------

    # wordcount_df = json_df.selectExpr("value.itemInfos.id",
    #                                  "value.itemInfos.text",
    #                                  "value.time_stamp") \
    #     .withColumn("timestamp", to_timestamp(from_unixtime(col("time_stamp").cast(IntegerType()),"yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
    #     .select(col("timestamp"), col("id"), explode(array_distinct(expr("split(text, ' ')"))).alias("words")) \
    #     .withWatermark("timestamp", "15 minute") \
    #     .groupBy(col("words"),
    #              window(col("timestamp"), "15 minute", "15 minute")) \
    #     .agg(collect_list(col("id")).alias("ids")) \
    #     .withColumn("TotalMentions", size(col("ids"))) \
    #     .withColumn("year", year(col("window.end"))) \
    #     .withColumn("month", month(col("window.end"))) \
    #     .withColumn("day", dayofmonth(col("window.end"))) \
    #     .drop("id", "text", "time_stamp")
