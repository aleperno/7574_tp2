from hashlib import sha1
from functools import lru_cache


@lru_cache
def workers_balancer(metric_id, worker_count):
    """
    Will output which worker index should be used based on a metric_id

    Example, a broker has 4 workers available, therefore we must spread all possible metric_ids
    between these 4 workers. To do this I do the following
     - Calculate a Hash of the metric_id
     - Get the fist char of the hash
     - Since the hash is made of hexadecimal values, we transform that char into a decimal ranging
        0-15
     - Then we divide this number (integer division) by the ammount of "division" each worker allows
     Example:
         If we have 2 workers, each worker will handle 8 possibilities 0-7 and 8-15
         So `a` -> 10 -> 10 // (16 // 2) -> 10 // 8 -> 1

         Meaning the worker index is 1

    For efficiency reasons we use a LRU cache to avoid doing this calculation each time
    """
    if isinstance(metric_id, str):
        metric_id = metric_id.encode()
    hash = sha1(metric_id).hexdigest()
    first_char = hash[0]  # Get the first char of the hash
    decimal_value = int(first_char, 16)  # Convert to decimal
    return decimal_value // (16 // worker_count)