import pika
import os
from src.constants import RABBIT_HOST
from src.utils.connections import connect_retry

POST_FILTER_REPLICAS = int(os.environ.get('POST_FILTER_REPLICAS', 1))


class Puppeteer:
    def __init__(self):
        self.conn = None
        self.channel = None

    def connect(self):
        self.conn = connect_retry(host=RABBIT_HOST)
        self.channel = self.conn.channel()

    def init(self):
        # Define queue to receive messages
        self.channel.queue_declare(queue='puppeteer', exclusive=True)
        # Where we will be consuming from
        self.channel.basic_consume(queue='puppeteer', on_message_callback=self.consume, auto_ack=True)
        # Define consumer for posts input
        self.init_puppet_pool(name='post_filter', replicas=POST_FILTER_REPLICAS, mapped=False)

    def run(self):
        self.channel.start_consuming()

    def init_puppet_pool(self, name, replicas, mapped=False):
        # Define exchange for the puppets
        exchange_name = f'{name}_exchange'
        init_queue_name = f'{name}_init_queue'
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='topic')
        # Define init queue to send data to he puppets
        self.channel.queue_declare(init_queue_name)
        # Bind the channel to the exchange
        self.channel.queue_bind(queue=init_queue_name, exchange=exchange_name, routing_key=init_queue_name)
        # For each replica, insert a message with the 'id' into the init queue
        # Also create the queue or queues for the given replicas, depending if they all consume from the same queue
        # or have independent queues
        for i in range(replicas):
            if not mapped and i==0:
                # Must define the queue
                queue_name = f'{name}_input'
                self.channel.queue_declare(queue=queue_name)
                self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key='0')
            elif mapped:
                # Each replica has its own queue
                queue_name = f'{name}_input_{i}'
                self.channel.queue_declare(queue=queue_name)
                self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=str(i))

            # Send ID to the puppet channel so they can consume it and assign to themselves
            print(f"Publico un {i}")
            self.channel.basic_publish(exchange=exchange_name, routing_key=init_queue_name, body=str(i))

    def consume(self, ch, method, properties, body):
        print("Puppeteer Orchestrator %r" % body)
