from src.common.puppet import Puppet
from src.common.messagig import Message, MessageEnum
from src.constants import (STUDENT_MEME_CALTULATOR_REPLICAS,
                           STUDENT_MEME_ALLOW_REPEATS,
                           STUDENT_MEME_CHUNKS,
                           RESULTS_EXCHANGE,
                           RESULT_STUDENT_MEMES_QUEUE,
                           RESULT_BEST_SENTIMENT_MEME_QUEUE,
                           )
from src.utils import get_post_id
from src.utils.balancer import workers_balancer
from time import time
from collections import defaultdict
from more_itertools import chunked


FILTER_MAPPING = {
    'id': 'post_id',
    'url': 'meme_url',
    'score': 'post_score',
}

DELETED_COMMENT = {'[deleted]', '[removed]'}
STUDENT_KEYWORDS = {'university', 'college', 'student', 'teacher', 'professor'}


class CommentFilter(Puppet):
    name = 'comment_filter'

    def __init__(self):
        super().__init__(mapped=False)
        self.start = None
        self.count = 0

    @staticmethod
    def process_data(entry):
        permalink = entry['permalink']
        comment = entry['body']
        sentiment = float(entry['sentiment'] or 0)
        score = int(entry['score'])

        post_id = get_post_id(permalink)

        if comment not in DELETED_COMMENT and post_id:
            return {
                'post_id': post_id,
                'comment': comment,
                'sentiment': sentiment,
                'score': score,
            }

    def consume(self, ch, method, properties, body):
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)
        if message.is_data:
            self.count += len(message.payload)
            data = []
            for entry in message.payload:
                processed = self.process_data(entry)
                if processed:
                    data.append(processed)

            if data:
                self.forward_to_students(data)
                self.forward_to_sentiment_meme(data)

            print(f"Enviados {self.count}")
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.notify_done()
            end = time()
            print(f"Termine en {end - self.start} segundos")

    def forward_to_students(self, data):
        new_data = defaultdict(list)
        for entry in data:
            routing_key = str(workers_balancer(entry['post_id'], STUDENT_MEME_CALTULATOR_REPLICAS))
            if int(routing_key) >= STUDENT_MEME_CALTULATOR_REPLICAS:
                print(entry)
                raise
            new_data[routing_key].append(entry)
        print(f"Enviando {len(data)} datos a {STUDENT_MEME_CALTULATOR_REPLICAS} replicas")
        for routing_key, dataset in new_data.items():
            msg = Message.create_data(payload=dataset)
            self.channel.basic_publish(exchange='student_meme_calculator_exchange',
                                       routing_key=routing_key,
                                       body=msg.dump())
            print(f"Envie {len(dataset)} a {routing_key}")

    def forward_to_sentiment_meme(self, data):
        msg = Message.create_data(payload=data)
        self.channel.basic_publish(exchange='sentiment_calculator_exchange',
                                   routing_key='0',
                                   body=msg.dump())


class StudentMemeCalculator(Puppet):
    name = 'student_meme_calculator'

    """
    self.data_mapping = {
        <post_id>: {'meme_url': <url>, 'post_score': None, 'student_count': 0}
    }
    """

    def __init__(self):
        super().__init__(mapped=True)
        self.start = None
        self.count = 0
        self.data_mapping = defaultdict(dict)
        self.posts_score_average = None

    def consume(self, ch, method, properties, body):
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)
        gen = []
        print("recibo un chunk de algo")
        if message.is_data:
            for i, chunk in enumerate(chunked(self.process_chunk(message.payload), STUDENT_MEME_CHUNKS)):
                # TODO REENVIARLO
                meme_list = list(chunk)
                print(f"Chunk n: {i+1}, total {len(meme_list)}")
                self.send_to_results(meme_list)
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.notify_done()
            end = time()
            print(f"Termine en {end - self.start} segundos")

    def send_to_results(self, payload):
        msg = Message.create_data(payload=payload)
        self.channel.basic_publish(exchange=RESULTS_EXCHANGE,
                                   routing_key=RESULT_STUDENT_MEMES_QUEUE,
                                   body=msg.dump())

    def process_chunk(self, chunk):
        for data in chunk:
            if 'posts_score_average' in data:
                # It's the posts average
                self.posts_score_average = data['posts_score_average']
                print(list(self.data_mapping.items())[:10])
                gen = self.process_average()
            elif 'post_score' in data:
                # Es informacion de un post
                gen = self.process_post(data)
            elif 'comment' in data:
                # Es un comment
                gen = self.process_comment(data)

            for meme in gen:
                yield meme

    def process_post(self, data):
        """
        Tengo que rellenar la info con la data de los posts. Y si algun comment se puede enviar,
        se envia
        """
        post_id = data['post_id']
        meme_url = data['meme_url']
        post_score = data['post_score']
        comment_data = self.data_mapping[post_id]

        comment_data.update({'meme_url': meme_url, 'post_score': post_score})
        for meme in self.get_student_memes(post_id):
            yield meme

    def process_comment(self, data):
        post_id = data['post_id']
        comment = data['comment'].lower()

        if any(keyword in comment for keyword in STUDENT_KEYWORDS):
            current_student_comments = self.data_mapping.get(post_id, {}).get('student_count', 0)
            self.data_mapping[post_id]['student_count'] = current_student_comments + 1
            if not STUDENT_MEME_ALLOW_REPEATS and current_student_comments > 0:
                # Do nothing
                pass
            else:
                for meme in self.get_student_memes(post_id):
                    yield meme

    def is_post_student_worthy(self, post_id):
        data = self.data_mapping.get(post_id)
        if data and self.posts_score_average:
            best_than_avg = data.get('post_score', -1) > self.posts_score_average
            student_count = data.get('student_count', 0)
            has_meme_url = data.get('meme_url', False)
            if best_than_avg and has_meme_url and student_count > 0:
                return data['meme_url'], student_count
        return None, 0

    def process_average(self):
        """
        Recordar que en este punto puedo (y es lo mas probable) tener info de posts
        y comments, completos o no
        """
        for post_id in self.data_mapping.keys():
            for meme in self.get_student_memes(post_id):
                yield meme

    def get_student_memes(self, post_id):
        meme_url, count = self.is_post_student_worthy(post_id)
        if meme_url:
            loops = count if STUDENT_MEME_ALLOW_REPEATS else 1
            for _ in range(loops):
                yield meme_url


class SentimentMeme(Puppet):
    name = 'sentiment_calculator'

    """
    self.data_mapping = {
        <post_id>: {'sentiment_sum': N, 'comment_count': N}
    }
    """

    def __init__(self):
        super().__init__(mapped=False)
        self.start = None
        self.count = 0
        self.data_mapping = defaultdict(dict)
        self.posts_score_average = None

    def consume(self, ch, method, properties, body):
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)

        print("recibo un chunk de algo")
        if message.is_data:
            self.process_chunk(message.payload)
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.process()
            self.notify_done()
            end = time()
            print(f"Termine en {end - self.start} segundos")

    def process_chunk(self, chunk):
        for data in chunk:
            if 'meme_url' in data:
                meme_url = data['meme_url']
                post_id = data['post_id']
                self.data_mapping[post_id].update({'meme_url': meme_url})
            elif 'comment' in data:
                post_id = data['post_id']
                sentiment = float(data['sentiment'])
                post_data = self.data_mapping[post_id]
                new_sentiment = post_data.get('sentiment_sum', 0) + sentiment
                new_count = post_data.get('comment_count', 0) + 1

                post_data.update({'sentiment_sum': new_sentiment, 'comment_count': new_count})

    def process(self):
        best_meme = None
        best_sentiment = None

        for post_id, data in self.data_mapping.items():
            try:
                sentiment_average = data['sentiment_sum'] / data['comment_count']
                meme_url = data['meme_url']
            except (ZeroDivisionError, KeyError):
                # Just to be sure
                print(f"Tuve un problema con {data}")
                continue
            else:
                if not best_sentiment or sentiment_average > best_sentiment:
                    best_meme = meme_url
                    best_sentiment = sentiment_average
        print(f"El resultado es {best_meme} con {best_sentiment}")
        self.send_result(best_meme)

    def send_result(self, meme):
        msg = Message.create_data(payload=meme)
        self.channel.basic_publish(exchange=RESULTS_EXCHANGE,
                                   routing_key=RESULT_BEST_SENTIMENT_MEME_QUEUE,
                                   body=msg.dump())

