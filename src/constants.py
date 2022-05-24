import os

POSTS_FILE_PATH = os.environ.get('POSTS_FILE_PATH', '/tmp/posts.csv')
POSTS_FILE_CHUNK = int(os.environ.get('POSTS_FILE_CHUNK', 10000))
COMMENT_FILE_PATH = os.environ.get('COMMENT_FILE_PATH', '/tmp/comments.csv')
COMMENT_FILE_CHUNK = int(os.environ.get('COMMENT_FILE_CHUNK', 10000))
RABBIT_HOST = os.environ.get('RABBIT_HOST', 'localhost')
POST_FILTER_REPLICAS = int(os.environ.get('POST_FILTER_REPLICAS', 1))
STUDENT_MEME_CALTULATOR_REPLICAS = int(os.environ.get('STUDENT_MEME_CALCULATOR_REPLICAS', 1))
