from time import sleep
from collections import defaultdict
from random import randint

from src.common.puppet import Puppet
from src.common.messagig import Message, MessageEnum


FILTER_MAPPING = {
    'id': 'post_id',
    'url': 'meme_url',
    'score': 'post_score',
}


class PostFilter(Puppet):
    name = 'post_filter'

    def __init__(self):
        super().__init__(mapped=False)

    def run(self):
        i = 0
        while True:
            msg = Message.create_data(payload={'post_score': randint(0, 100), 'post_id': i})
            i += 1
            self.channel.basic_publish(exchange='post_avg_calculator_exchange', routing_key='0', body=msg.dump())
            sleep(1)

    def consume(self, *args, **kwargs):
        raw_data = super().consume(*args, **kwargs)
        message = Message.from_bytes(raw_data)
        if message.is_data:
            data = message.payload
            post_score = data['post_score']

            self.posts_count += 1
            self.posts_score_sum += post_score
            print(f"Procese {data}, suma actual: {self.posts_score_sum}, total: {self.posts_count}")
        elif message.data_ready():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()


class PostAvgCalculator(Puppet):
    name = 'post_avg_calculator'

    def __init__(self):
        super().__init__(mapped=False)
        self.posts_count = 0
        self.posts_score_sum = 0

    def consume(self, *args, **kwargs):
        raw_data = super().consume(*args, **kwargs)
        message = Message.from_bytes(raw_data)
        if message.is_data:
            data = message.payload
            score_sum = data['post_score']

            self.posts_count += 1
            self.posts_score_sum += score_sum
            print(f"Procese {data}, suma actual: {self.posts_score_sum}, total: {self.posts_count}")
        elif message.data_ready():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            final_score_average = self.posts_score_sum / self.posts_count
            print(f"Resultado final {final_score_average}")
