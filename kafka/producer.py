#!/usr/bin/python3
from smart_open import open
import time
import lxml.etree

for i, line in enumerate(open('s3://stackoverflow-ds/PostHistory_rt.xml')):
    if i <= 1:  # Skip first two lines
        continue
    print(" ")
    print(line)
    

    schema = ["Comment", 'Id', 'PostHistoryTypeId', 'PostId', 'RevisionGUID', 'Text', 'UserDisplayName', 'UserId']
    parsed = {}

    for col in schema:
        value = lxml.etree.fromstring(line).xpath('//row/@{}'.format(col))
        parsed[col] = value[0] if len(value) > 0 else ""

    print(parsed)

    # Load into Kafka server

    if i % 5 == 0:
        print("resting 5 sec...")
        time.sleep(5)
    if i == 1000:
        break
