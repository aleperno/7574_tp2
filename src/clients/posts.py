from time import time
from src.clients.base import BaseClient
from src.constants import (POSTS_FILE_PATH, POSTS_FILE_CHUNK)
from src.utils.signals import SigTermException
from src.utils.csv import read_csv
from src.common.messagig import Message


class PostClient(BaseClient):
    def _main_loop(self):
        channel = self.channel

        try:
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
        except SigTermException:
            pass
        finally:
            channel.close()
