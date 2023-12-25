docker cp  .\bin\setup_cassandra.cql cassandra:/setup_cassandra.cql

docker exec -it cassandra cqlsh -f /setup_cassandra.cql
