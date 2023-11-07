#!/usr/bin/env python3

# AUTHOR: PRERANA SANJAY KULKARNI
# DATE: 27-10-23

import sys
from kafka import KafkaConsumer
import json

TOPIC_NAME = sys.argv[1]
KAFKA_SERVER = 'localhost:9092'
result = {}

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers = KAFKA_SERVER)

for message in consumer:
	line = message.value.decode()
	tokens = line.split()	
	
	if line == 'EOF':
		break
	
	user = tokens[2]
	
	if tokens[0] == 'comment':
		comment = str(" ".join(tokens[4:]))
		comment = comment[1:-1]
		
	if user not in result:
		result[user] = []
	result[user].append(comment)

result_sorted = dict(sorted(result.items()))
print(json.dumps(result_sorted, indent = 4))
	
