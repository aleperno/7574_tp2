from src.common.messagig import Message, MessageEnum
from src.common.puppet import Puppet
from src.constants import POSTS_FILE_PATH, POSTS_FILE_CHUNK, RABBIT_HOST, COMMENT_FILE_CHUNK, COMMENT_FILE_PATH
from src.utils.connections import connect_retry
from src.utils.csv import read_csv
from time import time


class PostClient:
    def main_loop(self):
        conn = connect_retry(host=RABBIT_HOST)
        channel = conn.channel()
        channel.exchange_declare(exchange='post_filter_exchange', exchange_type='topic')
        channel.queue_declare(queue='post_filter_input')
        channel.queue_bind(exchange='post_filter_exchange', queue='post_filter_input', routing_key='0')

        count = 0
        start = time()
        for chunk in read_csv(file_path=POSTS_FILE_PATH, chunksize=POSTS_FILE_CHUNK):
            msg = Message.create_data(payload=chunk)
            channel.basic_publish(
                exchange='post_filter_exchange',
                routing_key='0',
                body=msg.dump()
            )
            count += POSTS_FILE_CHUNK
            print(f"Enviado: {count}")

        end = time()
        print(f"Terminado en {end-start} segundos")
        msg = Message.create_eof()
        channel.basic_publish(
            exchange='post_filter_exchange',
            routing_key='0',
            body=msg.dump()
        )


class CommentClient:
    def main_loop(self):
        conn = connect_retry(host=RABBIT_HOST)
        channel = conn.channel()
        channel.exchange_declare(exchange='comment_filter_exchange', exchange_type='topic')
        channel.queue_declare(queue='comment_filter_input')
        channel.queue_bind(exchange='comment_filter_exchange', queue='comment_filter_input', routing_key='0')

        count = 0
        start = time()
        for chunk in read_csv(file_path=COMMENT_FILE_PATH, chunksize=COMMENT_FILE_CHUNK):
            msg = Message.create_data(payload=chunk)
            channel.basic_publish(
                exchange='comment_filter_exchange',
                routing_key='0',
                body=msg.dump()
            )
            count += POSTS_FILE_CHUNK
            print(f"Enviado: {count}")

        end = time()
        print(f"Terminado en {end-start} segundos")
        msg = Message.create_eof()
        channel.basic_publish(
            exchange='comment_filter_exchange',
            routing_key='0',
            body=msg.dump()
        )
