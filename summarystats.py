import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import mean, stddev, col

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Rolling Stats") \
        .master("local[3]") \
        .config('spark.driver.extraClassPath', '/Users/beccaboo/postgresql-42.2.18.jar') \
        .config('spark.executor.extraClassPath', '/Users/beccaboo/postgresql-42.2.18.jar') \
        .getOrCreate()

    window_length = 0.25
    stream_window_num = 24 / window_length

    # Read data from wordcount table
    raw_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/tiktok") \
        .option("dbtable", "wordcount") \
        .load()

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
    # now = datetime.datetime.now()
    # now_date = now.strftime("%Y-%m-%d")
    rolling_stats.write.format("jdbc").mode("append") \
    .option("url", "jdbc:postgresql://localhost/tiktok") \
    .option("dbtable", "wc_stats") \
    .save()