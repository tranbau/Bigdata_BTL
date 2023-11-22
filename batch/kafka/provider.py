from confluent_kafka import Producer
from json import dumps
from time import sleep
import pandas as pd
import json
import sys

data_path = '../../data/data.csv'
df = pd.read_csv(data_path)

producer = Producer({'bootstrap.servers': 'localhost:9092'})
try:
    for index, row in df.iterrows():
        dict_stock = row.to_dict()
        json_data = json.dumps(dict_stock).encode("utf-8")
        producer.produce('batch', value=json_data)
        sleep(5)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    producer.flush()  # Ensure all messages are sent
