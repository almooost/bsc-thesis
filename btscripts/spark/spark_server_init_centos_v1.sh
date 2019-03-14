#!/bin/bash
# Spark public and private keys
wget -P /home/ec2-user/.ssh/ https://s3.amazonaws.com/ffhs-bt-bds/keys/spark/id_rsa
wget -P /home/ec2-user/.ssh/ https://s3.amazonaws.com/ffhs-bt-bd/spark/id_rsa.pub
chown ec2-user /home/ec2-user/.ssh/id_*
chmod 600 /home/ec2-user/.ssh/id_*