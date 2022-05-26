from src.common.messagig import Message, MessageEnum
from src.common.puppet import Puppet
from src.common.puppeteer import Puppeteer
from src.constants import (POSTS_FILE_PATH,
                           POSTS_FILE_CHUNK,
                           RABBIT_HOST,
                           COMMENT_FILE_CHUNK,
                           COMMENT_FILE_PATH,
                           MEME_RESULT_PATH,
                           RESULTS_EXCHANGE,
                           RESULT_POST_SCORE_AVG_QUEUE,
                           RESULT_STUDENT_MEMES_QUEUE,
                           RESULT_BEST_SENTIMENT_MEME_QUEUE,
                           )

from src.utils.connections import connect_retry
from src.utils.csv import read_csv
from time import time


RESULT_QUEUES = (RESULT_BEST_SENTIMENT_MEME_QUEUE, RESULT_STUDENT_MEMES_QUEUE, RESULT_BEST_SENTIMENT_MEME_QUEUE)


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


class ResultClient:

    def __init__(self):
        self.results = {
            'post_avg': False,
            'student_memes': False,
            'sentiment_meme': False
        }
        self.channel = None

    def consume_post_avg(self, ch, method, properties, body):
        message = Message.from_bytes(body)

        if message.is_data():
            print(f"Posts Average is {message.payload['posts_score_average']}")
        elif message.eof():
            self.results['post_avg'] = True
            self.check_end()

    def consume_student_meme(self, ch, method, properties, body):
        message = Message.from_bytes(body)
        if message.is_data():
            for meme_url in message.payload:
                print(f"Student Meme URL: {meme_url}")
        elif message.eof():
            self.results['student_memes'] = True
            self.check_end()

    def consume_sentiment_meme(self, ch, method, properties, body):
        message = Message.from_bytes(body)

        if message.is_data():
            print(f"Best Sentiment Meme is {message.payload}")
        elif message.eof():
            self.results['sentiment_meme'] = True
            self.check_end()

    def check_end(self):
        if all(result_ready for result_key, result_ready in self.results.items()):
            self.channel.stop_consuming()
            self.notify_end()

    def notify_end(self):
        msg = Message.create_eof()
        self.channel.basic_publish(exchange='', routing_key='puppeteer', body=msg.dump())

    def main_loop(self):
        conn = connect_retry(host=RABBIT_HOST)
        self.channel = conn.channel()

        Puppeteer.create_result_sinks(self.channel)

        self.channel.basic_consume(queue=RESULT_BEST_SENTIMENT_MEME_QUEUE,
                                   on_message_callback=self.consume_sentiment_meme,
                                   auto_ack=True)

        self.channel.basic_consume(queue=RESULT_STUDENT_MEMES_QUEUE,
                                   on_message_callback=self.consume_student_meme,
                                   auto_ack=True)

        self.channel.basic_consume(queue=RESULT_POST_SCORE_AVG_QUEUE,
                                   on_message_callback=self.consume_post_avg,
                                   auto_ack=True)
