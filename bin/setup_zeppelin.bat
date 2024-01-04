@echo off
docker cp .\packages zeppelin:opt/zeppelin/interpreter/spark/dep/packages

docker-compose exec spark-master /spark/bin/spark-submit --master spark://f5e0d3014aba:7077 ./spark-streaming.py --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2