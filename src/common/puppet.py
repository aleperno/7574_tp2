import os
from functools import partial
from src.utils.connections import connect_retry
from src.common.messagig import Message, MessageEnum
from src.utils.signals import register_handler, SigTermException


RABBIT_HOST = os.environ.get('RABBIT_HOST', 'localhost')
mapper_id = 0


class Puppet:
    """
    Base class for a node, aka Puppet. These instances will communicate with one instance of a class
    called Puppeteer that will serve as a controlling agent.
    """
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
        """
        Returns the input queue name that will be used to consume data
        The name of the queue will be based on the name of the class
            {class_name}_input

        If the puppet is mapped, meaning each instance will receive independent data, the queue
        name will also contain the id of the instance
            {class_name}_input_{id}
        """
        basename = f'{self.name}_input'
        if self.mapped:
            return f'{basename}_{self._id}'
        else:
            return basename

    def connect(self):
        """
        Connects to the Rabbitmq and returns if the connection was successful
        """
        self.conn = connect_retry(host=RABBIT_HOST)
        if not self.conn:
            print("Failed to connect")
            return False
        else:
            self.channel = self.conn.channel()
            return True

    def init(self):
        """
        Performs the initial configuration of the instance
            - Setup exchange
            - Setup Init Queue
            - Get instance id
            - Setup data input queue and notifies puppeteer
        Will wait until we receive the instance id to continue the execution
        """
        # Declare an exchange to be used to control the puppets.
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')
        # Define the queue to be used to initialize all puppets of this kind
        self.channel.queue_declare(queue=self.init_queue_name)
        # Bind the queue to the exchange
        self.channel.queue_bind(queue=self.init_queue_name, exchange=self.exchange_name, routing_key=self.init_queue_name)
        # Start consuming from the init queue
        self.channel.basic_consume(queue=self.init_queue_name, on_message_callback=self.get_id_callback)
        print("Waiting instance id")
        self.channel.start_consuming()
        # From this point onwards we know the ID of the puppet
        self._start_consuming()

    def _start_consuming(self):
        """
        Setups the consumer for the data input queue
        """
        self.identifier = f'{self.name}_{self._id}'
        self.channel.basic_consume(queue=self.input_queue_name, on_message_callback=self.consume, auto_ack=True)
        # Flag the puppeteer we're ready
        msg = Message(type=MessageEnum.CONTROL.value, src=self.name, src_id=self._id, payload='ready')
        self.notify_puppeteer(body=msg.dump())

    def notify_puppeteer(self, body):
        self.channel.basic_publish(exchange='', routing_key='puppeteer', body=body)

    def notify_done(self):
        msg = Message.create_done(src=self.name, src_id=self._id)
        self.notify_puppeteer(body=msg.dump())

    def run(self):
        """
        Starts consuming from the input data queue
        """
        self.channel.start_consuming()

    def consume(self, ch, method, properties, body):
        return body

    def get_id_callback(self, ch, method, properties, body):
        """
        Setups the instance ID based on a message received on the init queue.
        Once received we stop consuming
        """
        self._id = int(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()

    def main_loop(self):
        """
        Represents the full life of the instance
        """
        # Register SigtermHandler
        register_handler()
        if not self.connect():
            # The connection failed therefore we cannot continue with the execution
            return
        # Perform the initial setup
        self.init()
        try:
            # Start consuming data
            self.run()
        except SigTermException:
            self.handle_sigterm()
        # Teardown
        self.channel.close()
        self.conn.close()

    def handle_sigterm(self):
        # Does nothing, the destruction of the exchanges and the queues will be done by the
        # pupetteer
        pass
