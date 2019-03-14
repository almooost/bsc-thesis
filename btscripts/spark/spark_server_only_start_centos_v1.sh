#!/bin/bash
# Start Master (Driver) and Slaves (Executors)
/opt/spark/spark-2.4.0-bin-hadoop2.7/sbin/start-master.sh &
/opt/spark/spark-2.4.0-bin-hadoop2.7/sbin/start-slaves.sh &