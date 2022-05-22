import os

RABBIT_HOST = os.environ.get('RABBIT_HOST', 'localhost')
POST_FILTER_REPLICAS = int(os.environ.get('POST_FILTER_REPLICAS', 1))
POST_AVG_REDUCERS_REPLICAS = int(os.environ.get('POST_AVG_REDUCERS', 1))