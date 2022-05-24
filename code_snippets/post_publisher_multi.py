#!/usr/bin/env python
import pika
import time
import sys
from src.utils.csv import read_csv, multiproc_read_csv
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


def f(df):
    data = df.to_dict(orient='records')
    msg = Message.create_data(payload=data)
    channel.basic_publish(
        exchange='',
        routing_key='testing',
        body=msg.dump()
    )
    print(len(data))

count = 0
start = time.time()
multiproc_read_csv(file_path=FILE, func=f, chunksize=10000, poolsize=1)
end = time.time()

print(f"Took {end-start} seconds")
