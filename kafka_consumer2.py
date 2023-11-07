#!/usr/bin/env python3

# AUTHOR: PRERANA SANJAY KULKARNI
# DATE: 27-10-23

import sys
import json
from kafka import KafkaConsumer

# Define the topic name for client2
TOPIC2 = sys.argv[2]

result = {}

consumer = KafkaConsumer(TOPIC2, bootstrap_servers='localhost:9092')

for message in consumer:
    line = message.value.decode()
    tokens = line.split()
    
    if line == 'EOF':
    	break
	
    action = tokens[0]
    user = tokens[2]
    post_id = tokens[3]

    if action == 'like':
        if user not in result:
            result[user] = {}
        if post_id not in result[user]:
            result[user][post_id] = 0
        result[user][post_id] += 1

result_sorted = dict(sorted(result.items()))
print(json.dumps(result_sorted, indent = 4))
