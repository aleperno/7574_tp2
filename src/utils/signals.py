import signal


class SigTermException(Exception):
    pass


def sigtermhandler(signum, frame):
    raise SigTermException


def register_handler():
    signal.signal(signal.SIGTERM, sigtermhandler)
