from time import time
from src.clients.base import BaseClient
from src.utils.csv import read_csv
from src.utils.signals import SigTermException
from src.constants import (COMMENT_FILE_PATH, COMMENT_FILE_CHUNK)
from src.common.messagig import Message


class CommentClient(BaseClient):
    def _main_loop(self):
        channel = self.channel

        try:
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
                count += COMMENT_FILE_CHUNK
                print(f"Enviado: {count}")

            end = time()
            print(f"Terminado en {end-start} segundos")
            msg = Message.create_eof()
            channel.basic_publish(
                exchange='comment_filter_exchange',
                routing_key='0',
                body=msg.dump()
            )
        except SigTermException:
            pass
        finally:
            channel.close()
