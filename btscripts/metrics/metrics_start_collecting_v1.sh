#!/bin/bash
# Start collectl for gathering metrics on machine
sudo /opt/collectl/collectl-4.3.1/collectl -i1 --all --export graphite,54.83.166.192 &