#!/bin/bash
# Update machine and install java, git and docker
sudo yum -y update
sudo yum install -y java-1.8.0-openjdk-headless.x86_64 git
# Create JAVA Home direcotry
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-0.amzn2.x86_64/
# Create new ssh key to export to slaves
cat /dev/zero | ssh-keygen -q -N "" > /dev/null
# Install collectl for collecting data
sudo mkdir /opt/collectl
sudo wget -P /opt/collectl https://sourceforge.net/projects/collectl/files/collectl/collectl-4.3.1/collectl-4.3.1.src.tar.gz
sudo tar xvzf /opt/collectl/collectl-4.3.1.src.tar.gz -C /opt/collectl/
# Create spark directory and download spark
sudo mkdir /opt/spark
sudo wget -P /opt/spark/ http://www.pirbot.com/mirrors/apache/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
sudo tar xvzf /opt/spark/spark*.tgz -C /opt/spark/
sudo mv /opt/spark/spark-2.4.0-bin-hadoop2.7/conf/slaves.template /opt/spark/spark-2.4.0-bin-hadoop2.7/conf/slaves
# Clone scripts repository and download jar file
sudo git clone https://github.com/almooost/btscripts /opt/btscripts
sudo chmod a+x -R /opt/btscripts/
sudo cp /opt/btscripts/spark/metrics.properties /opt/spark/spark-2.4.0-bin-hadoop2.7/conf/
# replace ip in start script for later usage
sudo /opt/btscripts/spark/replace_ip.sh "$(ifconfig eth0 | grep "inet " | awk -F'[: ]+' '{ print $3 }')"
# Download Spark Experiments
sudo mkdir /opt/files
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/spark/spark-exp001-1.0-SNAPSHOT-jar-with-dependencies.jar
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/spark/spark-exp002-1.0-SNAPSHOT-jar-with-dependencies.jar
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/spark/spark-exp003-1.0-SNAPSHOT-jar-with-dependencies.jar
sudo wget -P /opt/files/ https://s3.amazonaws.com/ffhs-bt-bd/spark/spark-exp004-1.0-SNAPSHOT-jar-with-dependencies.jar
# add collectl to autostart
sudo chmod +x /etc/rc.d/rc.local
sudo cat /opt/btscripts/datasink/rc.local >> /etc/rc.local
# Reboot server
reboot now