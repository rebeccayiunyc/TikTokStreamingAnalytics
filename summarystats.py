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