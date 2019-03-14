#!/bin/bash
# Update and install dependencies
sudo yum -y update
sudo yum -y install git
sudo amazon-linux-extras install docker
# Clone repository with all scripts and make them executable
sudo git clone https://github.com/almooost/btscripts /opt/btscripts
sudo chmod a+x -R /opt/btscripts/
# Download Docker machine
sudo systemctl start docker
# Enable docker to start on server startup
sudo systemctl enable docker
# get graphite container and run it every time docker starts
sudo docker run -d \
 --name graphite \
 --restart=always \
 -p 80:80 \
 -p 2003-2004:2003-2004 \
 -p 2023-2024:2023-2024 \
 -p 8125:8125/udp \
 -p 8126:8126 \
 graphiteapp/graphite-statsd:1.1.5-9
# Copy Configuration to local for easy editing
sudo mkdir /home/ec2-user/graphite
sudo docker cp graphite:/opt/graphite/conf /home/ec2-user/graphite
sudo docker cp graphite:/opt/graphite/webapp/graphite /home/ec2-user/graphite/webapp
# Stop graphite image again
sudo docker stop graphite
# make changes if needed
reboot now
