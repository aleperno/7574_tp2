from time import sleep
from collections import defaultdict
from random import randint

from src.constants import (STUDENT_MEME_CALTULATOR_REPLICAS,
                           RESULTS_EXCHANGE,
                           RESULT_POST_SCORE_AVG_QUEUE,
                           )
from src.common.puppet import Puppet
from src.common.messagig import Message, MessageEnum
from src.utils import transform_dataset
from src.utils.balancer import workers_balancer
from src.utils.forwarders import forward_to_students, forward_to_sentiment_meme
from time import time

FILTER_MAPPING = {
    'id': 'post_id',
    'url': 'meme_url',
    'score': 'post_score',
}


class PostFilter(Puppet):
    """
    Filters Post Data

    Given each post data filter out only the necessary keys
    """
    name = 'post_filter'

    def __init__(self):
        super().__init__(mapped=False)
        self.start = None
        self.count = 0

    def consume(self, ch, method, properties, body):
        """
        Reads from the input data queue and process the data
        """
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)
        if message.is_data:
            # Filter out the post data keys
            transformed = [transform_dataset(data=entry, mapping=FILTER_MAPPING, update={'id': 'P'}) for entry in message.payload]

            # Send to Student Meme Calculator
            forward_to_students(self.channel, transformed)
            forward_to_sentiment_meme(self.channel, transformed)

            # Send tu Post AVG Calculator
            msg = Message.create_data(payload=transformed)
            self.channel.basic_publish(exchange='post_avg_calculator_exchange', routing_key='0',
                                       body=msg.dump())

        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.notify_done()
            end = time()
            print(f"Finished in {end - self.start} seconds")


class PostAvgCalculator(Puppet):
    name = 'post_avg_calculator'

    def __init__(self):
        super().__init__(mapped=False)
        self.posts_count = 0
        self.posts_score_sum = 0
        self.start = None
        self.end = None

    def consume(self, ch, method, properties, body):
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)
        if message.is_data:
            data = message.payload
            score_sum = sum(entry['post_score'] for entry in data)

            self.posts_count += len(data)
            self.posts_score_sum += score_sum
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            final_score_average = self.posts_score_sum / self.posts_count
            self.forward_to_students(average=final_score_average)
            self.send_to_results(average=final_score_average)
            self.notify_done()
            end = time()
            print(f"Finished in {end - self.start} seconds")

    def forward_to_students(self, average):
        payload = {'posts_score_average': average, 'id': 'AVG'}
        for worker in range(STUDENT_MEME_CALTULATOR_REPLICAS):
            msg = Message.create_data(payload=[payload])
            self.channel.basic_publish(exchange='student_meme_calculator_exchange',
                                       routing_key=str(worker),
                                       body=msg.dump())

    def send_to_results(self, average):
        payload = {'posts_score_average': average}
        msg = Message.create_data(payload=payload)
        self.channel.basic_publish(exchange=RESULTS_EXCHANGE,
                                   routing_key=RESULT_POST_SCORE_AVG_QUEUE,
                                   body=msg.dump())
