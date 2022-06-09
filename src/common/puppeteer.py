import pika
import os
from src.constants import (RABBIT_HOST,
                           POST_FILTER_REPLICAS,
                           STUDENT_MEME_CALTULATOR_REPLICAS,
                           RESULT_POST_SCORE_AVG_QUEUE,
                           RESULT_STUDENT_MEMES_QUEUE,
                           RESULT_BEST_SENTIMENT_MEME_QUEUE)
from src.utils.connections import connect_retry
from src.posts import PostAvgCalculator, PostFilter
from src.comments import CommentFilter, StudentMemeCalculator, SentimentMeme
from src.common.messagig import Message
from collections import defaultdict
from src.utils.signals import register_handler, SigTermException

"""
This mapping will serve to notify all producer when a consumer is ready to receive data,
each key-value pair represents:
    key: The Consumer that is ready to receive data
    value: Set of producers to be unlocked
"""

RESULT_QUEUES = (RESULT_POST_SCORE_AVG_QUEUE, RESULT_STUDENT_MEMES_QUEUE, RESULT_BEST_SENTIMENT_MEME_QUEUE)
PRODUCER_CONSUMER_MAPPING = {
    PostFilter.name: (PostAvgCalculator.name, StudentMemeCalculator.name, SentimentMeme.name),
    CommentFilter.name: (StudentMemeCalculator.name, SentimentMeme.name),
    PostAvgCalculator.name: (StudentMemeCalculator.name, )
}

RESULTS_PRODUCERS = {
    PostAvgCalculator.name: RESULT_POST_SCORE_AVG_QUEUE,
    StudentMemeCalculator.name: RESULT_STUDENT_MEMES_QUEUE,
    SentimentMeme.name: RESULT_BEST_SENTIMENT_MEME_QUEUE,
}

CONSUMER_PRODUCER_MAPPING = defaultdict(set)

for producer, consumers in PRODUCER_CONSUMER_MAPPING.items():
    for consumer in consumers:
        CONSUMER_PRODUCER_MAPPING[consumer].add(producer)


class Puppeteer:
    """
    Puppeteer Class

    This will serve as an orchestrator of multiple elements in our program
    """
    def __init__(self):
        self.conn = None
        self.channel = None
        self.control_pool = defaultdict(dict)
        self.queues_mapper = defaultdict(dict)

    def connect(self):
        self.conn = connect_retry(host=RABBIT_HOST)
        if not self.conn:
            print("Failed to connect")
            return False
        else:
            self.channel = self.conn.channel()
            return True

    def init(self):
        # Define queue to receive messages
        self.channel.queue_declare(queue='puppeteer', exclusive=True)
        # Where we will be consuming from
        self.channel.basic_consume(queue='puppeteer', on_message_callback=self.consume, auto_ack=True)

        # From this point we will be defining all the other elements in our system
        # Define where the results will be stored at
        self.create_result_sinks(self.channel)
        # Define the Students Meme Analyzer
        self.init_puppet_pool(name=StudentMemeCalculator.name,
                              replicas=STUDENT_MEME_CALTULATOR_REPLICAS, notify=True, mapped=True)
        # Define Sentiment Meme Calculator
        self.init_puppet_pool(name=SentimentMeme.name, notify=True)
        # Define Posts Average Calculator
        self.init_puppet_pool(name=PostAvgCalculator.name, replicas=1, notify=True)
        # Define Posts Filters
        self.init_puppet_pool(name=PostFilter.name, replicas=POST_FILTER_REPLICAS, notify=True)
        # Define Comments Filters
        self.init_puppet_pool(name=CommentFilter.name, replicas=1, notify=True)

    def run(self):
        self.channel.start_consuming()

    def init_puppet_pool(self, name, replicas=1, mapped=False, notify=False):
        """
        Perform the setup of a pool of puppets

        Args:
            - name: The name of the puppet class
            - replicas: The ammount of replicas that will be instanciated
            - mapped (bool): Set to True if each replica will have its independent queue
        """
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
                routing_key ='0'
                self.channel.queue_declare(queue=queue_name)
                self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)
            elif mapped:
                # Each replica has its own queue
                queue_name = f'{name}_input_{i}'
                self.channel.queue_declare(queue=queue_name)
                routing_key = str(i)
                self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)

            self.control_pool[name].update({i: {'status': 'init',
                                           'mapped': mapped,
                                           'exchange': exchange_name,
                                           'queue_name': queue_name,
                                           'routing_key': routing_key}})

            if notify:
                # Send ID to the puppet channel so they can consume it and assign to themselves
                print(f"Sending {i} to {init_queue_name}")
                self.channel.basic_publish(exchange=exchange_name, routing_key=init_queue_name, body=str(i))

    def consume(self, ch, method, properties, body):
        """
        Will consume from the input queue and process notifications from the puppets and clients
        """
        msg = Message.from_bytes(body)
        if msg.data_ready():
            # The sender is ready to receive data
            if msg.src not in self.control_pool:
                print(f"{msg.src} not found in control pool")
            else:
                # Change the status in the control pool
                self.control_pool[msg.src][msg.src_id]['status'] = 'ready'
            # Check if all replicas are ready to receive data
            if all(data['status'] == 'ready' for data in self.control_pool[msg.src].values()):
                # If they are all ready I can delete the INIT queue
                init_queue = f'{msg.src}_init_queue'
                self.channel.queue_delete(queue=init_queue)
                print(f"Removing queue {init_queue}")
                pass
        elif msg.control_done():
            # The sender completed it tasks
            data = self.control_pool[msg.src][msg.src_id]
            data['status'] = 'done'
            source = msg.src
            if data['mapped']:
                # Since the puppet is mapped (each instance has it's own queue), we can delete the
                # input queue without checking the other instances status
                print(f"Deleting {data['queue_name']}")
                self.channel.queue_delete(queue=data['queue_name'])
            if all(data['status'] == 'done' for data in self.control_pool[msg.src].values()):
                # All instances finished their tasks
                # Therefore we can delete the puppet exchange and data input queues

                # Delete the exchange
                self.channel.exchange_delete(exchange=f"{source}_exchange")

                # Delete each of the queues of the instances
                # If they're not mapped and all instances use the same queue, this doesn't
                # fail since the removal of a queue is idempotent and doesnt raise any Exceptions
                for _id, worker_data in self.control_pool[msg.src].items():
                    queue_name = worker_data['queue_name']
                    print(f'Deleting queue: {queue_name}')
                    self.channel.queue_delete(queue=queue_name)

                # If this source produced data for the results, we can send an eof to the results
                # to mark its completion
                result_queue = RESULTS_PRODUCERS.get(source)
                if result_queue:
                    self.notify_eof(exchange='results', routing_key=result_queue)

                # If they have consumers, we could tell them no more data is available
                for consumer_name in PRODUCER_CONSUMER_MAPPING.get(source, tuple()):
                    # However a consumer has multiple producers, therefore before telling them
                    # there is no more data, we must validate all producers have ended
                    all_producers_status = (data['status']=='done' for producer in CONSUMER_PRODUCER_MAPPING[consumer_name] for data in self.control_pool[producer].values())
                    if all(all_producers_status):
                        for consumer_id, consumer_data in self.control_pool[consumer_name].items():
                            self.notify_eof(exchange=consumer_data['exchange'],
                                            routing_key=consumer_data['routing_key'])
        elif msg.eof():
            self.delete_result_sinks()
            self.channel.stop_consuming()

    def notify_eof(self, exchange, routing_key):
        msg = Message.create_eof().dump()
        kwargs = {
            'exchange': exchange,
            'routing_key': routing_key,
            'body': msg
        }
        self.channel.basic_publish(**kwargs)

    @staticmethod
    def create_result_sinks(channel):
        channel.exchange_declare(exchange='results', exchange_type='direct')
        for result_queue in RESULT_QUEUES:
            channel.queue_declare(queue=result_queue)
            channel.queue_bind(queue=result_queue, exchange='results', routing_key=result_queue)

    def delete_result_sinks(self):
        for result_queue in RESULT_QUEUES:
            self.channel.queue_delete(queue=result_queue)

        self.channel.exchange_delete(exchange='results')

    def teardown(self):
        for node_name, data in self.control_pool.items():
            exchange = ''
            for replica_n, config in data.items():
                self.channel.queue_delete(queue=config['queue_name'])
                exchange = config['exchange']
            self.channel.exchange_delete(exchange=exchange)

    def main_loop(self):
        if not self.connect():
            # The connection failed therefore we cannot continue with the execution
            return
        self.init()
        register_handler()
        try:
            self.run()
        except SigTermException:
            self.teardown()
            self.delete_result_sinks()
        self.channel.close()
        self.conn.close()
