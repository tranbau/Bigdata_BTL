const WebSocketServer = require("websocket").server;
const http = require("http");

// Kafka config
const kafka = require("kafka-node");
const Consumer = kafka.Consumer;
const Producer = kafka.Producer;
const client = new kafka.KafkaClient({ kafkaHost: "localhost:8097" });

// Kafka consumer
const consumer = new Consumer(
  client,
  [
    { topic: "data", partition: 0 },
    { topic: "data", partition: 1 },
  ],
  {
    autoCommit: false,
  }
);

// Kafka producer
const producer = new Producer(client);
producer.on("ready", function () {
  console.log("Kafka Producer is ready");
});
producer.on("error", function (err) {
  console.error("Kafka Producer Error: " + err);
});

// Socket server
const server = http.createServer(function (request, response) {
  console.log(" Request recieved : " + request.url);
  response.writeHead(404);
  response.end();
});
server.listen(8888, function () {
  console.log("Listening on port : 8888");
});
const webSocketServer = new WebSocketServer({
  httpServer: server,
  autoAcceptConnections: false,
});
function iSOriginAllowed(origin) {
  return true;
}
webSocketServer.on("request", function (request) {
  if (!iSOriginAllowed(request.origin)) {
    request.reject();
    console.log(" Connection from : " + request.origin + " rejected.");
    return;
  }

  const connection = request.accept("echo-protocol", request.origin);
  console.log(" Connection accepted : " + request.origin);

  connection.on("message", function (message) {
    if (message.type === "utf8") {
      console.log("Received Message: " + message.utf8Data);

      // Send to kafka config topic
      const payloads = [
        { topic: "config", messages: message.utf8Data, partition: 0 },
      ];

      producer.send(payloads, function (err, data) {
        if (err) {
          console.error("Error sending test message to Kafka: ", err);
        } else {
          console.log("Send message successfully", data);
        }
      });
    }
  });

  consumer.on("message", function (message) {
    console.log(message);
    connection.sendUTF(message.value);
  });

  connection.on("close", function (reasonCode, description) {
    console.log("Connection " + connection.remoteAddress + " disconnected.");
  });
});
