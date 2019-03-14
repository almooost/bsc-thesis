#!/bin/bash
# add ssh keys to server
wget -P /home/ec2-user/.ssh/ https://s3.amazonaws.com/ffhs-bt-bds/keys/flink/id_rsa
wget -P /home/ec2-user/.ssh/ https://s3.amazonaws.com/ffhs-bt-bd/flink/id_rsa.pub
chown ec2-user /home/ec2-user/.ssh/id_*
chmod 600 /home/ec2-user/.ssh/id_*