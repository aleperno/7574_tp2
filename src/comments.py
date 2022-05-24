from src.common.puppet import Puppet
from src.common.messagig import Message, MessageEnum
from src.constants import STUDENT_MEME_CALTULATOR_REPLICAS
from src.utils import get_post_id
from src.utils.balancer import workers_balancer
from time import time
from collections import defaultdict


FILTER_MAPPING = {
    'id': 'post_id',
    'url': 'meme_url',
    'score': 'post_score',
}

DELETED_COMMENT = {'[deleted]', '[removed]'}


class CommentFilter(Puppet):
    name = 'comment_filter'

    def __init__(self):
        super().__init__(mapped=False)
        self.start = None
        self.count = 0

    @staticmethod
    def process_data(entry):
        permalink = entry['permalink']
        comment = entry['body']
        sentiment = float(entry['sentiment'])

        post_id = get_post_id(permalink)

        if comment not in DELETED_COMMENT and post_id:
            return {
                'post_id': post_id,
                'comment': comment,
                'sentiment': sentiment
            }

    def consume(self, ch, method, properties, body):
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)
        if message.is_data:
            self.count += len(message.payload)
            data = []
            for entry in message.payload:
                processed = self.process_data(entry)
                if processed:
                    data.append(processed)

            if data:
                self.forward_to_students(data)

            print(f"Enviados {self.count}")
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.notify_done()
            end = time()
            print(f"Termine en {end - self.start} segundos")

    def forward_to_students(self, data):
        new_data = defaultdict(list)
        for entry in data:
            routing_key = str(workers_balancer(entry['post_id'], STUDENT_MEME_CALTULATOR_REPLICAS))
            if int(routing_key) >= STUDENT_MEME_CALTULATOR_REPLICAS:
                print(entry)
                raise
            new_data[routing_key].append(entry)
        print(f"Enviando {len(data)} datos a {STUDENT_MEME_CALTULATOR_REPLICAS} replicas")
        for routing_key, dataset in new_data.items():
            msg = Message.create_data(payload=dataset)
            self.channel.basic_publish(exchange='student_meme_calculator_exchange',
                                       routing_key=routing_key,
                                       body=msg.dump())
            print(f"Envie {len(dataset)} a {routing_key}")


class StudentMemeCalculator(Puppet):
    name = 'student_meme_calculator'

    def __init__(self):
        super().__init__(mapped=True)
        self.start = None
        self.count = 0

    def consume(self, ch, method, properties, body):
        pass
