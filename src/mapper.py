import pika
import os
from src.utils.connections import connect_retry

RABBIT_HOST = os.environ.get('RABBIT_HOST', 'localhost')
mapper_id = 0


def callback(ch, method, properties, body):
    print("[x] Mapper %r" % body)


def get_mapper_id(ch, method, properties, body):
    global mapper_id
    mapper_id = body.decode()
    print(f"Mapper recibio {body}")
    ch.basic_publish(exchange='', routing_key='orchestrator', body=f'ok_{mapper_id}')
    ch.stop_consuming()


def main():
    connection = connect_retry(host=RABBIT_HOST)
    print("arranco un mapper")
    # This is to avoid race conditions
    channel = connection.channel()
    channel.queue_declare(queue='mappers_setup')

    channel.basic_consume(queue='mappers_setup', on_message_callback=get_mapper_id, auto_ack=True)
    channel.start_consuming()
    print(f"El mapper possee id {mapper_id}")

    channel.basic_consume(queue=f"map_{mapper_id}", on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
