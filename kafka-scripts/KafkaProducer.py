from kafka import KafkaProducer
import ndjson
import pandas as pd

if __name__ == "__main__":

    # Instantiate a KafkaProducer example for delivering messages to Kafka
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092',
                            client_id = 'TiktokKafkaproducer',
                            acks = 'all')
                            #compression_type = 'snappy')

    with open('/Users/beccaboo/Documents/GitHub/TikTok/Spark-kafka-stream/datagen/samples.json') as f:
        reader = ndjson.reader(f)

        for post in reader:
            line = str(post)
            producer.send(topic ='tiktok', value = line.encode())

    producer.close()