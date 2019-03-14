#!/bin/python
# read file line by line and keep decrasing sleep time
from time import sleep

f = open('/opt/files/reviews_other.json')
speed = float(0.00001) # 0.00001 = 100'000/s
for line in f:
    print line
    sleep(speed)
#    speed = float(speed / 1.00001) # Uncomment for Exp003
f.close()