@echo off
docker exec -it kafka1 /bin/bash -c "kafka-topics --bootstrap-server localhost:9092 --create --topic data --partitions 2 --replication-factor 2"
docker exec -it kafka1 /bin/bash -c "kafka-topics --bootstrap-server localhost:9092 --create --topic config --partitions 2 --replication-factor 2"
docker cp .\packages zeppelin:opt/zeppelin/interpreter/spark/dep/packages
