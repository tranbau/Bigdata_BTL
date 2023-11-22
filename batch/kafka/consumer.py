from confluent_kafka import Consumer, KafkaException
import pandas as pd
import sys
import json
import psycopg2


consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                         'group.id': 'batch',
                         'auto.offset.reset': 'latest',
                         'enable.auto.commit': False})
consumer.subscribe(['batch'])
    
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            record = json.loads(msg.value())
            print(record)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down the consumer to commit final offsets.
    consumer.close()

    