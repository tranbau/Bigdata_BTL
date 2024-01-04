from flask import Flask, jsonify, request, render_template
from flask_socketio import SocketIO,emit
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import threading

app = Flask(__name__, template_folder='templates')
CORS(app,resources={r"/*":{"origins":"*"}})
socketio = SocketIO(app,cors_allowed_origins="*")

# kafka config
TOPIC_CONFIG = "config"
TOPIC_DATA = "data"
KAFKA_SERVER = ['localhost:8097']
CONSUMER_GROUP = "bigdata"

# producer
producer = KafkaProducer(
    bootstrap_servers= KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('ascii'),
    key_serializer=lambda v: json.dumps(v).encode('ascii')
)

@app.route('/', methods=['GET', 'POST'])
def index():
    selected_price = request.form.get("price", "0")
    
    if request.method == 'POST':
        producer.send(
            TOPIC_CONFIG,
            value={
                "name": "google",
                "priceConfig": float(selected_price),
                "timestampConfig": int(time.time())
            }
        )
        producer.flush()
    elif request.method == 'GET':
        return render_template('index.html', form=request.form, selected_price=selected_price)

    return render_template("index.html")

@app.route('/chart')
def showChart():
    return render_template('chart.html')

@socketio.on("connect")
def connected():
    print(request.sid)
    print("client has connected")
    emit("connect",{"data":f"id: {request.sid} is connected"})
@socketio.on("disconnect")
def disconnected():
    print("user disconnected")
    emit("disconnect",f"user {request.sid} disconnected",broadcast=True)


# consumer
consumer = KafkaConsumer(
    client_id="client",
    group_id= CONSUMER_GROUP,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='latest',
)
consumer.subscribe(topics=[TOPIC_DATA])
def listen_kafka():
    for message in consumer:
        if message is not None: 
            print(message)
            socketio.emit('kafka_message', message.value.decode('utf8'))

    
if __name__ == '__main__':
    kafka_thread = threading.Thread(target=listen_kafka)
    kafka_thread.daemon = True
    kafka_thread.start()
    
    socketio.run(app, debug=True,port=5000)
