#!/usr/bin/python3
from smart_open import open
import time
import lxml.etree
import json
from datetime import datetime
from kafka import KafkaProducer
from random import randint

producer = KafkaProducer(bootstrap_servers='172.31.7.229:9092')

n = randint(5, 50)  # Random generate n events in 5 sec
next_pause = n

for i, line in enumerate(open('s3://stackoverflow-ds/PostHistory_rt.xml')):
    if i <= 1:  # Skip first two lines
        continue
    print(" ")
    print(line)

    schema = ["Comment", 'Id', 'PostHistoryTypeId', 'PostId', 'RevisionGUID', 'Text', 'UserDisplayName', 'UserId']
    row = {}

    for col in schema:
        value = lxml.etree.fromstring(line).xpath('//row/@{}'.format(col))
        row[col] = value[0] if len(value) > 0 else ""

    now = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S.000')
    row['_CreationDate'] = now 
    parsed = json.dumps(row)
    print(parsed)

    # Load into Kafka server
    producer.send('posthistory', parsed.encode('utf-8'))

    if i == next_pause:
        print("resting 5 sec...")
        next_pause += randint(5, 20)
        time.sleep(5)
    
    if i == 100000:
        break
