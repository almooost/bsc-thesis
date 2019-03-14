#!/bin/bash
# Start Kafka Server and the necessary topics for the experiments
sudo /opt/kafka/kafka_2.11-2.1.0/bin/zookeeper-server-start.sh /opt/kafka/kafka_2.11-2.1.0/config/zookeeper.properties &
sudo /opt/kafka/kafka_2.11-2.1.0/bin/kafka-server-start.sh /opt/kafka/kafka_2.11-2.1.0/config/server.properties &
sudo /opt/kafka/kafka_2.11-2.1.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 8 --partitions 24 --topic test &
sudo /opt/kafka/kafka_2.11-2.1.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 8 --partitions 24 --topic test-other &