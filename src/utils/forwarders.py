from collections import defaultdict
from src.utils.balancer import workers_balancer
from src.constants import STUDENT_MEME_CALTULATOR_REPLICAS
from src.common.messagig import Message


def forward_to_students(channel, data):
    new_data = defaultdict(list)

    for entry in data:
        routing_key = str(workers_balancer(entry['post_id'], STUDENT_MEME_CALTULATOR_REPLICAS))
        new_data[routing_key].append(entry)

    for routing_key, dataset in new_data.items():
        msg = Message.create_data(payload=dataset)
        channel.basic_publish(exchange='student_meme_calculator_exchange',
                              routing_key=routing_key,
                              body=msg.dump())


def forward_to_sentiment_meme(channel, data):
    msg = Message.create_data(payload=data)
    channel.basic_publish(exchange='sentiment_calculator_exchange',
                          routing_key='0',
                          body=msg.dump())
