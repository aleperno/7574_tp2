#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs', exchange_type='topic')
channel.queue_declare(queue='testing')
channel.queue_bind(queue='testing', exchange='direct_logs', routing_key='testing')

for i in range(2):
    channel.basic_publish(
        exchange='direct_logs',
        routing_key='testing',
        body=str(i))
    print(f"Publique {i}")
connection.close()
