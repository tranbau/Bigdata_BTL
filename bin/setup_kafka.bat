@echo off
docker exec -it kafka1 /bin/bash -c "kafka-topics --bootstrap-server localhost:9092 --create --topic data --partitions 2 --replication-factor 2"