#!/bin/bash
# Send file to kafka consumer via kafka console-producer
sudo /opt/btscripts/datasink/read_file.py | sudo /opt/kafka/kafka_2.11-2.1.0/bin/kafka-console-producer.sh --broker-list 52.207.6.107:9092 --topic test $1