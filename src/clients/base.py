from src.utils.signals import register_handler
from src.utils.connections import connect_retry
from src.constants import RABBIT_HOST


class BaseClient:
    def __init__(self):
        self.conn = None
        self.channel = None

    def _main_loop(self):
        raise NotImplementedError

    def main_loop(self):
        register_handler()
        self.conn = connect_retry(host=RABBIT_HOST)
        if not self.conn:
            print("Failed to setup connection")
        else:
            self.channel = self.conn.channel()
            self._main_loop()
