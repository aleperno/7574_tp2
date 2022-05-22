from retry import retry
import pika


@retry(pika.exceptions.AMQPConnectionError, delay=5, backoff=2, tries=5)
def connect_retry(host):
    """
    Connect to a rabbitmq with a retry mechanism

    We'll run rabbit and its client as docker containers. Since the `depends_on` only tests the container being up and not
    the actual service, we have to implement a retry mechanism so clients keep trying to connnect while the rabbitmq service
    starts.

    Currently we use the retry lib (not builtin) with a default delay of 5 seconds and 5 max retries. Each retry will double
    the time before the next try.
    """
    return pika.BlockingConnection(pika.ConnectionParameters(host=host))
