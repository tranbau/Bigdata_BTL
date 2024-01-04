from confluent_kafka import Consumer, KafkaException, KafkaError
import pandas as pd
import sys
import json
from cassandra.cluster import Cluster
import psycopg2

# DB connection
conn = psycopg2.connect( database="stock", user='user', password='password', host='127.0.0.1', port= '5432')
cursor = conn.cursor()
 
def msg_process(msg):
    record = json.loads(msg.value())
    metadata = {
        "record": record,
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp": msg.timestamp()
    }
    # store data to DB
    insert_query = "INSERT INTO stock_data (date, open, high, low, close,adjClose, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    
    # Extracting values from the record
    date = record.get('date')
    open = record.get('open')
    high = record.get('high')
    low = record.get('low')
    close = record.get('close')
    ajdClose = record.get("adjClose")
    volume = record.get("volume")
    
    # Insert data into the table
    cursor.execute(insert_query, (date, open,high,low, close, ajdClose, volume))
    conn.commit()

    print("Consumer1", metadata, end='\n')


# Auto commit
def auto_commit_consume(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition 
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    except KeyboardInterrupt:
        
        sys.stderr.write('%% Aborted by user\n')
    finally:
        consumer.close()

def syn_commit_consume(consumer, topics):
    MIN_COMMIT_COUNT = 10
    try:
        consumer.subscribe(topics)

        msg_count = 0
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        
        sys.stderr.write('%% Aborted by user\n')
    finally:
        consumer.close()
        conn.close()
        
def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))     
           
def asyn_commit_consume(consumer, topics):
    MIN_COMMIT_COUNT = 10
    try:
        consumer.subscribe(topics)

        msg_count = 0
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition 
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        consumer.close()
            
if __name__ == '__main__':
    topics = ["data"]
    config = {
        'bootstrap.servers': 'localhost:8097',
        'group.id': 'batch',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }    
    try:
        consumer = Consumer(config)
        auto_commit_consume(consumer=consumer, topics=topics)
    except Exception as e:
        print("Error:", e)
    
    # To handle with asyn commit
    # config = {
    #     'bootstrap.servers': 'localhost:8097',
    #     'group.id': 'batch',
    #     'auto.offset.reset': 'earliest',
    #     'enable.auto.commit': False,
    #     'on_commit': commit_completed
    # }