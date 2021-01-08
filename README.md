## Project Overview
This project presents a real-time anomaly detection system that monitors unusual activities on social media platform, TikTok.
### Introduction
Brands in modern world face challenges of timely community management and reputation maintanence on multiple social outlets. Words have a quick way to travel across the Internet and consumer sentiments are prone to rapid decline if brands do not take proper actions immediately upon emerging topics and crises. When social media sites such as Twitter and Facebook have a relatively easy way to keep track of the latest activities of a given keyword, the trending Gen-Z's favorite platform, Tiktok, does not provide a **latest** view of search, which makes community management harder for brand strategists.  

This project presents a streaming pipeline system which ingests and processes Tiktok data in a real-time manner and detects abnormal activities of a given keyword. In addition, it also sinks streaming data to S3 and further stores them in ProgreSQL dabatases. Airflow then orchestrates tasks and runs daily batch analysis to gain insights on trending challenges, musicians, etc.

### Dataset
The dataset is kindly provided by Jason@Pushift.io and is about 95GB, stored as .ndjson format. It has a nested structure with four main fields, `authorInfos`, `challengeInfoList`, `itemInfos`, and `musicInfos`. A fifth field, `time_stamp` will be added as Kafka producer sends the message to simulate a streaming situation. 

## Architecture
![Alt text](/tiktok_pipeline.png?raw=true "piepline structure")
### Tools
1. Apache Kafka
2. Apache Spark Spark & Spark Streaming
3. S3
4. Postgresql
5. AWS EC2
6. Apache Airflow

### Conversion
### Transformation

## Result
### Engineering Challenge
