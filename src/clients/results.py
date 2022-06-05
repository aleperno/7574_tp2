from src.clients.base import BaseClient
from src.common.messagig import Message
from src.utils.downloader import download_and_store
from src.utils.signals import SigTermException
from src.constants import (MEME_RESULT_BUFFER,)
from src.common.puppeteer import Puppeteer

from src.constants import (RESULT_POST_SCORE_AVG_QUEUE, RESULT_BEST_SENTIMENT_MEME_QUEUE, RESULT_STUDENT_MEMES_QUEUE)


class ResultClient(BaseClient):
    def __init__(self):
        super().__init__()
        self.results = {
            'post_avg': False,
            'student_memes': False,
            'sentiment_meme': False
        }

    def consume_post_avg(self, ch, method, properties, body):
        message = Message.from_bytes(body)

        if message.is_data:
            print(f"Posts Average is {message.payload['posts_score_average']}")
        elif message.eof():
            self.results['post_avg'] = True
            self.check_end()

    def consume_student_meme(self, ch, method, properties, body):
        message = Message.from_bytes(body)
        if message.is_data:
            for meme_url in message.payload:
                print(f"Student Meme URL: {meme_url}")
                download_and_store(url=meme_url, buffersize=MEME_RESULT_BUFFER)
        elif message.eof():
            self.results['student_memes'] = True
            self.check_end()

    def consume_sentiment_meme(self, ch, method, properties, body):
        message = Message.from_bytes(body)

        if message.is_data:
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

    def _main_loop(self):
        try:
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

            self.channel.start_consuming()
        except SigTermException:
            pass
        finally:
            self.channel.close()
