from confluent_kafka import Consumer, KafkaException
import pandas as pd
import sys
import json
from cassandra.cluster import Cluster

# Cassandra connect 
cluster = Cluster(['localhost'], port=9042)  # Replace with your Cassandra node IP
session = cluster.connect('stock_data')  # Replace 'stock_data' with your keyspace name

topic = "data"
query = """
        INSERT INTO stock_prices (date, open, high, low, close, adj_close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                         'group.id': 'batch',
                         'auto.offset.reset': 'latest',
                         'enable.auto.commit': False})
consumer.subscribe([topic])
    
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            record = json.loads(msg.value())
            session.execute(query, (record['date'], record['open'], record['high'], record['low'],
                                     record['close'], record['adjClose'], record['volume']))

            print("Record inserted into Cassandra:", record)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down the consumer to commit final offsets.
    consumer.close()

    