#!/bin/bash
sed -i s/localhost_private_ip/$1/g /opt/btscripts/spark/spark_server_start_centos_v1.sh
sed -i s/localhost_private_ip/$1/g /opt/btscripts/spark/spark_server_program_start_centos_v1.sh