#!/bin/bash
KAFKA_TOPICS='/opt/kafka/bin/kafka-topics.sh';
ZOOKEEPER_URL=zookeeper:2181

if [[ 2 = $($KAFKA_TOPICS --list --zookeeper $ZOOKEEPER_URL | grep -c 'graph-') ]]; then
  exit 0;
fi

echo "Creating topics:" \
&& bash $KAFKA_TOPICS --create --zookeeper $ZOOKEEPER_URL --replication-factor 1 --partitions 10 --topic graph-queue \
&& bash $KAFKA_TOPICS --create --zookeeper $ZOOKEEPER_URL --replication-factor 1 --partitions 10 --topic graph-done;
