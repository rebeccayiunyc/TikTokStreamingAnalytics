## Project Overview
This project presents a real-time anomaly detection system that monitors unusual activities on social media platform, TikTok.
### Introduction
Brands in modern world face challenges of timely community management and reputation maintanence on multiple social outlets. Words have a quick way to travel across the Internet and consumer sentiments are prone to rapid decline if brands do not take proper actions immediately upon emerging topics and crises. When social media sites such as Twitter and Facebook have a relatively easy way to keep track of the latest activities of a given keyword, the trending Gen-Z's favorite platform, Tiktok, does not provide a **latest** view of search, which makes community management harder for brand strategists.  

This project presents a streaming pipeline system which ingests and processes Tiktok data in a real-time manner and detects abnormal activities of a given keyword. In addition, it also sinks streaming data to S3 and further stores them in ProgreSQL dabatases. Airflow then orchestrates tasks and runs daily batch analysis to gain insights on trending challenges, musicians, etc.

### Dataset
The dataset is kindly provided by pushshift.io and is about 95GB, stored as .ndjson format. It has a nested structure with four main fields, `authorInfos`, `challengeInfoList`, `itemInfos`, and `musicInfos`. A fifth field, `time_stamp` will be added as Kafka producer sends the message to simulate a streaming situation. 


## Architecture
![Alt text](/tiktok_pipeline.png?raw=true "piepline structure")
### Toolkit
1. Apache Kafka
2. Apache Spark Spark & Spark Streaming
3. S3
4. Postgresql
5. AWS EC2
6. Apache Airflow

### Pipeline Flow
1. `KafkaProducer.py` sends raw json messages to Kafka and adds a streaming timestamp to data
2. `TikTokSparkStream.py` consumes Kafka content, sinks the streaming data to data lake in S3 for backup, and joins with historical statistics to detect outliers
3. `static_tiktok.py` reads data from S3 and parses it into structured data which are then stored in a Postgres database, which can be further queried for ad-hoc analysis.
4. `summarystats.py` reads data from Postgres database and generates historical statistics for outlier detection, and finally loads data to a Postgres table. 
5. `tiktok_dag` schedules daily batch job with Airflow on bullets 3 and 4. 


### Environment Setup 

### Next-steps
1. Refines text processing steps to clean out unhelpful content to save space for database 
2. Add a GUI component 
3. Improve outlier detection algorithm that factors in attributes such as seasonality and macro trends

