#!/bin/bash
/opt/flink/flink-1.7.0/bin/start-cluster.sh &
/opt/flink/flink-1.7.0/bin/flink run /opt/files/flink-exp001-1.0-SNAPSHOT-jar-with-dependencies.jar