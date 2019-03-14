#!/bin/bash
# This script is only necessary if docker does not run and graphite was not downloaded
# Run docker machine
sudo systemctl start docker
# Run docker machine if not already running
sudo docker run -d \
 --name graphite \
 --restart=always \
 -p 80:80 \
 -p 2003:2003 \
 -p 2004:2004 \
 -p 8125:8125/udp \
 -p 8126:8126 \
 -v /home/ec2-user/graphite/storage:/opt/graphite/storage \
 -v /home/ec2-user/log:/var/log \
 -v /home/ec2-user/graphite/conf:/opt/graphite/conf \
 -v /home/ec2-user/graphite/webapp/graphite:/opt/graphite/webapp/graphite \
 graphiteapp/graphite-statsd