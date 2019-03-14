#!/bin/bash
# Start spark experiment only
#
# --class = Class with main method
# --master = Where the master is (standalone mode = localhost)
# --driver-memory = Memory of spark master (driver)
# --driver-cores = Cores of spark master (driver)
# --executor-memory = Max. Memory used by a single executor
# --executor-cores = Cores of spark executor
# --total-executor-cores = Max executor cores
# --num-executors = Amout of executors to use
# --conf* = Additional configurations for application
# --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 for kafka data source
/opt/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
  --class ch.ffhs.alfano.spark.SparkExp001 \
  --master spark://localhost_private_ip:7077 \
  --driver-memory 8G \
  --driver-cores 4 \
  --executor-memory 8G \
  --executor-cores 4 \
  --total-executor-cores 16 \
  --num-executors 4 \
  --conf spark.default.parallelism=4 \
  --conf spark.streaming.backpressure.enabled=false \
  --conf spark.sql.streaming.checkpointLocation=hdfs://3.208.67.119:8020/spark \
  --conf spark.metrics.conf=/opt/spark/spark-2.4.0-bin-hadoop2.7/conf/metrics.properties \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 \
  /opt/files/spark-exp001-1.0-SNAPSHOT-jar-with-dependencies.jar