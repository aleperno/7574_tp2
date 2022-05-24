#!/usr/bin/env python
import pika
import time
import sys
from src.utils.csv import read_csv
from src.common.messagig import Message

FILE = '/home/apernin/Downloads/the-reddit-irl-dataset-posts.csv'

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
#channel.basic_qos(prefetch_count=1)

#hannel.exchange_declare(exchange='direct_logs', exchange_type='topic')
channel.queue_declare(queue='testing')
#channel.queue_bind(queue='testing', exchange='direct_logs', routing_key='testing')

CHUNKSIZE=10000

count = 0
start = time.time()
for chunk in read_csv(FILE, chunksize=CHUNKSIZE):
    #for row in chunk:
    msg = Message.create_data(payload=chunk)
    channel.basic_publish(
        exchange='',
        routing_key='testing',
        body=msg.dump()
    )
    count += CHUNKSIZE
    print(f"Llevo enviados {count} rows")
end = time.time()

print(f"Took {end-start} seconds")
