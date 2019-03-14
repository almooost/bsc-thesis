#!/bin/bash
# Update server
yum -y update
# Download and install java, git and docker
yum install -y java-1.8.0-openjdk-headless.x86_64 git
# Create java home variable
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.191.b12-0.amzn2.x86_64/jre
# Download collectl
sudo mkdir /opt/collectl
sudo wget -P /opt/collectl https://sourceforge.net/projects/collectl/files/collectl/collectl-4.3.1/collectl-4.3.1.src.tar.gz
sudo tar xvzf /opt/collectl/collectl-4.3.1.src.tar.gz -C /opt/collectl/
# Create hadoop directory, download and extract sources
sudo mkdir /opt/hadoop
sudo wget -P /opt/hadoop/ http://mirror.easyname.ch/apache/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz
sudo tar xvzf /opt/hadoop/hadoop-2.9.2.tar.gz -C /opt/hadoop/
# Clone scripts repository and copy necessary files1
sudo git clone https://github.com/almooost/btscripts /opt/btscripts
sudo chmod a+x -R /opt/btscripts/
# Change hadoop config
sudo sed -i s#\$\{JAVA_HOME\}#\/usr\/lib\/jvm\/java-1.8.0-openjdk-1.8.0.191.b12-0.amzn2.x86_64\/jre#g /opt/hadoop/hadoop-2.9.2/etc/hadoop/hadoop-env.sh
sudo cat /opt/btscripts/hadoop/core-site.xml > /opt/hadoop/hadoop-2.9.2/etc/hadoop/core-site.xml
sudo mkdir /opt/hadoop/hadoop-2.9.2/logs
sudo chown ec2-user /opt/hadoop/hadoop-2.9.2/logs
sudo chmod 777 /opt/hadoop/hadoop-2.9.2/logs
# add collectl to autostart
sudo chmod +x /etc/rc.d/rc.local
sudo cat /opt/btscripts/datasink/rc.local >> /etc/rc.local
# Reboot server
reboot now