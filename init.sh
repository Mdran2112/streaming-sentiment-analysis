docker compose up -d

sleep 5

docker compose exec broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic messages
docker compose exec broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic predictions
docker compose exec broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic errors

docker logs pyspark-streaming-streaming-1 -f
