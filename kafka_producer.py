#!/usr/bin/env python3

# AUTHOR: PRERANA SANJAY KULKARNI
# DATE: 27-10-23

import sys
from kafka import KafkaProducer

TOPIC1 = sys.argv[1]
TOPIC2 = sys.argv[2]
TOPIC3 = sys.argv[3]

KAFKA_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER, value_serializer = lambda v: str(v).encode('utf-8'))

for line in sys.stdin:
	line = line.strip()
	tokens = line.split(" ")
	if tokens[0] == 'comment':
		producer.send(TOPIC1, value = line)
		producer.send(TOPIC3, value = line)
	elif tokens[0] == 'like':
		producer.send(TOPIC2, value = line)
		producer.send(TOPIC3, value = line)
	elif tokens[0] == 'share':
		producer.send(TOPIC3, value = line)
	else:
		producer.send(TOPIC1, value = line)
		producer.send(TOPIC2, value = line)
		producer.send(TOPIC3, value = line)

producer.close()
	
