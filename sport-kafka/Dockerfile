FROM wurstmeister/kafka

MAINTAINER danielkec
#https://dzone.com/articles/dont-use-apache-kafka-consumer-groups-the-wrong-wa
CMD ["start-kafka.sh && kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic graph-done"]