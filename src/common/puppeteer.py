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
    def __init__(self):
        self.conn = None
        self.channel = None
        self.control_pool = defaultdict(dict)
        self.queues_mapper = defaultdict(dict)

    def connect(self):
        self.conn = connect_retry(host=RABBIT_HOST)
        self.channel = self.conn.channel()

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
        print(f"Este es mi control pool {self.control_pool}")

    def run(self):
        self.channel.start_consuming()

    def init_puppet_pool(self, name, replicas=1, mapped=False, notify=False):
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
        print(f"{name} has {replicas} replicas")
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

            self.control_pool[name] = {i: {'status': 'init',
                                           'mapped': mapped,
                                           'exchange': exchange_name,
                                           'queue_name': queue_name,
                                           'routing_key': routing_key}}

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
                self.control_pool[msg.src][msg.src_id]['status'] = 'ready'
            # Check if all replicas are ready to receive data
            if all(data['status']=='ready' for data in self.control_pool[msg.src].values()):
                # If they are all ready I can delete the INIT queue
                init_queue = f'{msg.src}_init_queue'
                self.channel.queue_delete(queue=init_queue)
                print(f"Removing queue {init_queue}")
                #self.unlock_producers(consumer=msg.src)
                pass
        elif msg.control_done():
            # The sender completed it tasks
            data = self.control_pool[msg.src][msg.src_id]
            data['status'] = 'done'
            source = msg.src
            print(f"Recibo que {msg.src}_{msg.src_id} termino")
            if data['mapped']:
                # I can delete the input queue
                print(f"Borro {data['queue_name']}")
                self.channel.queue_delete(queue=data['queue_name'])
            if all(data['status'] == 'done' for data in self.control_pool[msg.src].values()):
                # Todos terminaron su tarea
                # Algo seguro puedo hacer, borrar las queues de input?
                # TODO: Quiza se pueda borrar el exchange?
                print("terminaron todos")
                self.channel.exchange_delete(exchange=f"{source}_exchange")
                for _id, worker_data in self.control_pool[msg.src].items():
                    queue_name = worker_data['queue_name']
                    print(f'Borro queue: {queue_name}')
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
                        print(f"Puedo borrar {consumer_name}")
                        for consumer_id, consumer_data in self.control_pool[consumer_name].items():
                            self.notify_eof(exchange=consumer_data['exchange'],
                                            routing_key=consumer_data['routing_key'])
        elif msg.eof():
            self.channel.stop_consuming()
        print(f"Este es mi control pool {self.control_pool}")

    def notify_eof(self, exchange, routing_key):
        msg = Message.create_eof().dump()
        kwargs = {
            'exchange': exchange,
            'routing_key': routing_key,
            'body': msg
        }
        print(f'Mando {kwargs}')
        self.channel.basic_publish(**kwargs)

    #TODO: Deprecate
    def unlock_producers(self, consumer):
        for producer_name in CONSUMER_PRODUCER_MAPPING[consumer]:
            exchange_name = f'{producer_name}_exchange'
            init_queue_name = f'{producer_name}_init_queue'
            for i in self.control_pool[producer_name].keys():
                print(f"Publico un {i} en {init_queue_name}")
                self.channel.basic_publish(exchange=exchange_name, routing_key=init_queue_name, body=str(i))

    @staticmethod
    def create_result_sinks(channel):
        channel.exchange_declare(exchange='results', exchange_type='direct')
        for result_queue in RESULT_QUEUES:
            channel.queue_declare(queue=result_queue)
            channel.queue_bind(queue=result_queue, exchange='results', routing_key=result_queue)

    def main_loop(self):
        self.connect()
        self.init()
        self.run()
