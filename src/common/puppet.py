import os
from functools import partial
from src.utils.connections import connect_retry
from src.common.messagig import Message, MessageEnum

RABBIT_HOST = os.environ.get('RABBIT_HOST', 'localhost')
mapper_id = 0


class Puppet:
    name = 'basePuppet'

    def __init__(self, mapped=False):
        self._id = 0
        self.conn = None
        self.channel = None
        self.mapped = mapped
        self.queue_name = f'{self.name}'
        self.exchange_name = f'{self.name}_exchange'
        self.init_queue_name = f'{self.name}_init_queue'
        self.identifier = None

    @property
    def input_queue_name(self):
        basename = f'{self.name}_input'
        if self.mapped:
            return f'{basename}_{self._id}'
        else:
            return basename

    def connect(self):
        self.conn = connect_retry(host=RABBIT_HOST)
        if not self.conn:
            print("Failed to connect")
        else:
            self.channel = self.conn.channel()

    def init(self):
        # Declare an exchange to be used to control the puppets.
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')
        # Define the queue to be used to initialize all puppets of this kind
        self.channel.queue_declare(queue=self.init_queue_name)
        # Bind the queue to the exchange
        self.channel.queue_bind(queue=self.init_queue_name, exchange=self.exchange_name, routing_key=self.init_queue_name)
        # Start consuming from the init queue
        self.channel.basic_consume(queue=self.init_queue_name, on_message_callback=self.get_id_callback)
        print("Empiezo a consumir")
        self.channel.start_consuming()
        # From this point onwards we know the ID of the puppet
        self._start_consuming()

    def _start_consuming(self):
        print(f"{self.name} con id {self._id}")
        self.identifier = f'{self.name}_{self._id}'
        self.channel.basic_consume(queue=self.input_queue_name, on_message_callback=self.consume, auto_ack=True)
        # Flag the puppeteer we're ready
        print("flagging we're ready")
        msg = Message(type=MessageEnum.CONTROL.value, src=self.name, src_id=self._id, payload='ready')
        self.notify_puppeteer(body=msg.dump())

    def notify_puppeteer(self, body):
        self.channel.basic_publish(exchange='', routing_key='puppeteer', body=body)

    def notify_done(self):
        msg = Message.create_done(src=self.name, src_id=self._id)
        self.notify_puppeteer(body=msg.dump())

    def run(self):
        self.channel.start_consuming()

    def consume(self, ch, method, properties, body):
        print(f"[{self.name}_{self._id}] Received {body}")
        return body

    def get_id_callback(self, ch, method, properties, body):
        self._id = int(body.decode())
        print(f"Mapper recibio {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        #ch.basic_publish(exchange='', routing_key='puppeteer', body=f'ok_{self._id}')
        ch.stop_consuming()

    def main_loop(self):
        self.connect()
        self.init()
        self.run()


class PostFilter(Puppet):
    name = 'post_filter'
