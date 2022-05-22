import pika
import os
from retry import retry

MAP_REPLICAS = int(os.environ.get('MAP_REPLICAS', 1))
RABBIT_HOST = os.environ.get('RABBIT_HOST', 'localhost')


def callback(ch, method, properties, body):
    print("[x] Orchestrator %r" % body)


@retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()
    # This is the main queue
    channel.queue_declare(queue='orchestrator')
    # Queue for mappers Setup
    channel.queue_declare(queue='mappers_setup')

    channel.exchange_declare(exchange='mappers', exchange_type='direct')
    print("Creating map queues")
    for i in range(MAP_REPLICAS):
        print(f"Orquestador envia mensaje al mapper {i}")
        queue_name = f"map_{i}"
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(exchange='mappers', queue=queue_name, routing_key=str(i))
        channel.basic_publish(exchange='', routing_key='mappers_setup', body=str(i))

    channel.basic_consume(queue='orchestrator', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
