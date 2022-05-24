from time import sleep
from collections import defaultdict
from random import randint

from src.common.puppet import Puppet
from src.common.messagig import Message, MessageEnum
from src.utils import transform_dataset
from time import time

FILTER_MAPPING = {
    'id': 'post_id',
    'url': 'meme_url',
    'score': 'post_score',
}


class PostFilter(Puppet):
    name = 'post_filter'

    def __init__(self):
        super().__init__(mapped=False)
        self.start = None
        self.count = 0

    """
    def run(self):
        for i in range(30):
            msg = Message.create_data(payload={'post_score': randint(0, 100), 'post_id': i})
            i += 1
            self.channel.basic_publish(exchange='post_avg_calculator_exchange', routing_key='0', body=msg.dump())
            sleep(1)
        # Terminé de mandar datos, le aviso al puppeteer que terminé
        self.notify_done()
    """

    def consume(self, ch, method, properties, body):
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)
        if message.is_data:
            self.count += len(message.payload)

            transformed = [transform_dataset(data=entry, mapping=FILTER_MAPPING) for entry in message.payload]

            #data = transform_dataset(data=transformed, mapping=FILTER_MAPPING)
            #print(f"Recibi {message.payload} y envio {data}")
            msg = Message.create_data(payload=transformed)
            self.forward_data(msg)

            print(f"Enviados {self.count}")
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.notify_done()
            end = time()
            print(f"Termine en {end - self.start} segundos")

    def forward_data(self, msg):
        # TODO: Esto se podria hacer dinamicamente quiza, no me parece que esté bien que un 'puppet'
        # sepa de otro. Solo debería conocer al 'puppeteer' y en todo caso ser éste el que le provea
        # la información que necesita saber
        self.channel.basic_publish(exchange='post_avg_calculator_exchange', routing_key='0',
                                   body=msg.dump())


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
            #print(f"Procese {data}, suma actual: {self.posts_score_sum}, total: {self.posts_count}")
            print(f"Posts procesados: {self.posts_count}")
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            final_score_average = self.posts_score_sum / self.posts_count
            print(f"Resultado final {final_score_average}")
            # TODO: Ver donde devolver bien el resultado y ademas enviarlo al resto del
            # flujo del problema
            self.notify_done()
            end = time()
            print(f"Termine en {end - self.start} segundos")
