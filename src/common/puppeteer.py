import pika
import os
from src.constants import RABBIT_HOST, POST_FILTER_REPLICAS, POST_AVG_REDUCERS_REPLICAS
from src.utils.connections import connect_retry
from src.posts import PostAvgCalculator, PostFilter
from src.common.messagig import Message
from collections import defaultdict

"""
This mapping will serve to notify all producer when a consumer is ready to receive data,
each key-value pair represents:
    key: The Consumer that is ready to receive data
    value: Set of producers to be unlocked
"""
CONSUMER_PRODUCER_MAPPING = {
    PostAvgCalculator.name: (PostFilter.name, )
}


class Puppeteer:
    def __init__(self):
        self.conn = None
        self.channel = None
        self.control_pool = defaultdict(dict)

    def connect(self):
        self.conn = connect_retry(host=RABBIT_HOST)
        self.channel = self.conn.channel()

    def init(self):
        # Define queue to receive messages
        self.channel.queue_declare(queue='puppeteer', exclusive=True)
        # Where we will be consuming from
        self.channel.basic_consume(queue='puppeteer', on_message_callback=self.consume, auto_ack=True)
        # Define Posts Average Calculator
        self.init_puppet_pool(name=PostAvgCalculator.name, replicas=1, notify=True)
        # Define Posts Average Reducers
        self.init_puppet_pool(name=PostFilter.name, replicas=POST_FILTER_REPLICAS, notify=True)
        print(f"Este es mi control pool {self.control_pool}")

    def run(self):
        self.channel.start_consuming()

    def init_puppet_pool(self, name, replicas, mapped=False, notify=False):
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
            self.control_pool[name] = {i: 'init'}
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
            if notify:
                # Send ID to the puppet channel so they can consume it and assign to themselves
                print(f"Publico un {i} en {init_queue_name}")
                self.channel.basic_publish(exchange=exchange_name, routing_key=init_queue_name, body=str(i))

    def notify_ready(self, name, replicas):
        """
        Send an id to each of the replicas, this will flag the consumer they can start consuming
        """
        exchange_name = f'{name}_exchange'
        init_queue_name = f'{name}_init_queue'
        for i in range(replicas):
            # Send ID to the puppet channel so they can consume it and assign to themselves
            print(f"Publico un {i} en {init_queue_name}")
            self.channel.basic_publish(exchange=exchange_name, routing_key=init_queue_name, body=str(i))

    def consume(self, ch, method, properties, body):
        print("Puppeteer Orchestrator %r" % body)
        msg = Message.from_bytes(body)
        if msg.data_ready():
            # The sender is ready to receive data
            if msg.src not in self.control_pool:
                print(f"{msg.src} not found in control pool")
            else:
                self.control_pool[msg.src][msg.src_id] = 'ready'
            # Check if all replicas are ready to receive data
            if all(status=='ready' for status in self.control_pool[msg.src].values()):
                # Notiy blocked producers
                #self.unlock_producers(consumer=msg.src)
                pass

    def unlock_producers(self, consumer):
        for producer_name in CONSUMER_PRODUCER_MAPPING[consumer]:
            exchange_name = f'{producer_name}_exchange'
            init_queue_name = f'{producer_name}_init_queue'
            for i in self.control_pool[producer_name].keys():
                print(f"Publico un {i} en {init_queue_name}")
                self.channel.basic_publish(exchange=exchange_name, routing_key=init_queue_name, body=str(i))
