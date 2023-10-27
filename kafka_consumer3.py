#!/usr/bin/env python3

# AUTHOR: PRERANA SANJAY KULKARNI
# DATE: 27-10-23

import sys
import json
from kafka import KafkaConsumer

# Define the topic name for client3
TOPIC3 = sys.argv[1]

result = {}

consumer = KafkaConsumer(TOPIC3, bootstrap_servers='localhost:9092', group_id='client-3')

for message in consumer:
    line = message.value.decode()

    tokens = line.split()
    if len(tokens) < 4:
        continue

    action = tokens[0]
    user = tokens[2]
    post_id = tokens[3]

    if action == 'like' or action == 'share' or action == 'comment':
        if user not in result:
            result[user] = {'likes': 0, 'shares': 0, 'comments': 0}

        if action == 'like':
            result[user]['likes'] += 1
        elif action == 'share':
            result[user]['shares'] += len(tokens) - 4
        elif action == 'comment':
            result[user]['comments'] += 1

sorted_result = dict(sorted(result.items()))
popularity_result = {}

for user, data in sorted_result.items():
    popularity = (data['likes'] + 20 * data['shares'] + 5 * data['comments']) / 1000
    popularity_result[user] = popularity

print(json.dumps(popularity_result, indent=4))
