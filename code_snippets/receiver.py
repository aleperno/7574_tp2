#!/usr/bin/env python
import pika
import sys
import os
from src.utils.signals import register_handler, SigTermException

register_handler()
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
#channel.basic_qos(prefetch_count=1)

#hannel.exchange_declare(exchange='direct_logs', exchange_type='topic')
channel.queue_declare(queue='testing')
#channel.queue_bind(queue='testing', exchange='direct_logs', routing_key='testing')


def callback(ch, method, properties, body):
    #print(" [x] %r:%r" % (method.routing_key, body))
    pass


channel.basic_consume(
    queue='testing', on_message_callback=callback, auto_ack=True)
print(f"Mi PID es {os.getpid()}")
try:
    channel.start_consuming()
except SigTermException:
    print("Recibi un sigterm")
