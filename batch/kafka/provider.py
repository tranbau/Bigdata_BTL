from confluent_kafka import Producer
from json import dumps
from time import sleep
import pandas as pd
import json
import sys
import os

topic = "data"

script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to the CSV file relative to the script's directory
csv_file_path = os.path.join(script_dir, 'data.csv')

# Read the CSV file using pandas
df = pd.read_csv(csv_file_path)

producer = Producer({'bootstrap.servers': 'localhost:8097'})

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
        
try:
    for index, row in df.iterrows():
        dict_stock = row.to_dict()
        json_data = json.dumps(dict_stock).encode("utf-8")
        producer.produce(topic, value = json_data,callback = delivery_callback )
        sleep(1)
        producer.poll(0)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    producer.flush()  # Ensure all messages are sent
