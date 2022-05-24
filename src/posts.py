from time import sleep
from collections import defaultdict
from random import randint

from src.constants import STUDENT_MEME_CALTULATOR_REPLICAS
from src.common.puppet import Puppet
from src.common.messagig import Message, MessageEnum
from src.utils import transform_dataset
from src.utils.balancer import workers_balancer
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
            # Send to Student Meme Calculator
            self.forward_to_students(transformed)
            # Send tu Post AVG Calculator
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
            self.forward_to_students(average=final_score_average)

            self.notify_done()
            end = time()
            print(f"Termine en {end - self.start} segundos")

    def forward_to_students(self, average):
        payload = {'posts_score_average': average}
        for worker in range(STUDENT_MEME_CALTULATOR_REPLICAS):
            print(f"Enviando average {worker} ")
            msg = Message.create_data(payload=[payload])
            self.channel.basic_publish(exchange='student_meme_calculator_exchange',
                                       routing_key=str(worker),
                                       body=msg.dump())
