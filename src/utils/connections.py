from retry import retry
from pika.exceptions import AMQPConnectionError
from src.constants import (CONNECTION_RETRIES, CONNECTION_RETRY_DELAY, CONNECTION_RETRY_BACKOFF)
import pika


@retry(exceptions=AMQPConnectionError, delay=CONNECTION_RETRY_DELAY, backoff=CONNECTION_RETRY_BACKOFF, tries=CONNECTION_RETRIES)
def _connect_retry(host):
    """
    Connect to a rabbitmq with a retry mechanism

    We'll run rabbit and its client as docker containers. Since the `depends_on` only tests the container being up and not
    the actual service, we have to implement a retry mechanism so clients keep trying to connnect while the rabbitmq service
    starts.

    Currently we use the retry lib (not builtin) with a default delay of 5 seconds and 5 max retries. Each retry will double
    the time before the next try.
    """
    return pika.BlockingConnection(pika.ConnectionParameters(host=host))


def connect_retry(host):
    try:
        return _connect_retry(host)
    except AMQPConnectionError:
        return None
