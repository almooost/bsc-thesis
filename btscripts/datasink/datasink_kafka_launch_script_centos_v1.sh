#!/bin/bash
yum -y update
yum install -y java-1.8.0-openjdk-headless.x86_64 git
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-0.amzn2.x86_64/
# Download Kafka and set necessary configration parameters
mkdir /opt/kafka
wget -P /opt/kafka/ http://mirror.easyname.ch/apache/kafka/2.1.0/kafka_2.11-2.1.0.tgz 
tar xvzf /opt/kafka/kafka*.tgz -C /opt/kafka/
sudo sed -i 's/104857600/2066182517/g' /opt/kafka/kafka_2.11-2.1.0/config/server.properties
# Download setup/start scripts
sudo git clone https://github.com/almooost/btscripts /opt/btscripts
sudo chmod a+x -R /opt/btscripts/
# Download necessary data files for streaming
sudo mkdir /opt/files
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/data/aggressive_dedup.json.gz
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/data/reviews_Electronics.json.gz &
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/data/reviews_Books.json.gz &
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/data/reviews_Books_5.json.gz &
sudo cd /opt/files/
sudo gunzip /opt/files/*.gz
sudo mv /opt/files/reviews_Books.json /opt/files/reviews.json
sudo cp /opt/files/reviews.json /opt/files/reviews_other.json
# Reboot for a clean start of the service
reboot now
