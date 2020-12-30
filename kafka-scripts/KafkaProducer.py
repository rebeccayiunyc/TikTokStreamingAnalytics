from kafka import KafkaProducer
import time
import ndjson
import pandas as pd

if __name__ == "__main__":

    # Instantiate a KafkaProducer to push S3 data to Kafka
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092',
                            client_id = 'TiktokKafkaproducer',
                            acks = 'all')
                            #compression_type = 'snappy')

    with open('/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/datagen/samples.json') as f:
        reader = ndjson.reader(f)

        for post in reader:
            post['time_stamp'] = time.time()
            line = str(post)
            producer.send(topic ='tiktok', value = line.encode())

    producer.close()