#!/usr/bin/env python
import pika
from time import sleep
from src.common.messagig import Message

MSG_PER_SECONDS = 7000
TEST_PAYLOAD = {
    "type": "comment",
    "id": "f0yshst",
    "subreddit.id": "2s5ti",
    "subreddit.name": "meirl",
    "subreddit.nsfw": False,
    "created_utc": 1569077264,
    "permalink": "https://old.reddit.com/r/meirl/comments/d75qqr/meirl/f0yshst/",
    "body": "Lozenge",
    "sentiment": None,
    "score": 3
}

SLEEP_TIME = 1 / MSG_PER_SECONDS

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='testing')

while True:
    msg = Message.create_data(payload=TEST_PAYLOAD)
    channel.basic_publish(
        exchange='',
        routing_key='testing',
        body=msg.dump()
    )
    #sleep(SLEEP_TIME)

