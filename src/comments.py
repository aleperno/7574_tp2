from src.common.puppet import Puppet
from src.common.messagig import Message
from src.constants import (STUDENT_MEME_CALTULATOR_REPLICAS,
                           STUDENT_MEME_ALLOW_REPEATS,
                           STUDENT_MEME_CHUNKS,
                           RESULTS_EXCHANGE,
                           RESULT_STUDENT_MEMES_QUEUE,
                           RESULT_BEST_SENTIMENT_MEME_QUEUE,
                           )
from src.utils import get_post_id
from src.utils.balancer import workers_balancer
from src.utils.forwarders import forward_to_students, forward_to_sentiment_meme
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
    """
    Comments Filter

    This class will receive comments from a client and filter the required keys to be used in the
    rest of the program
    """
    name = 'comment_filter'

    def __init__(self):
        super().__init__(mapped=False)
        self.start = None
        self.count = 0

    @staticmethod
    def process_data(entry):
        """
        Performs the transformation of a Comment row

        Returns a subset of the data that will be useful for the next of the program.
        Filter out comments marked as deleted
        """
        permalink = entry['permalink']
        comment = entry['body']
        sentiment = float(entry['sentiment'] or 0)
        score = int(entry['score'])

        post_id = get_post_id(permalink)

        if comment not in DELETED_COMMENT and post_id:
            return {
                'id': 'C',
                'post_id': post_id,
                'comment': comment,
                'sentiment': sentiment,
                'score': score,
            }

    def consume(self, ch, method, properties, body):
        """
        Consumes from the data input

        For each of the comments, process them (through the filter) then forward the data
        to
            - Student Meme Processing
            - Sentiment Meme Processing
        """
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
                forward_to_students(self.channel, data)
                forward_to_sentiment_meme(self.channel, data)

        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.notify_done()
            end = time()
            print(f"Finished in {end - self.start} seconds")


class StudentMemeCalculator(Puppet):
    """
    Calculates the Memes liked by students
    """
    name = 'student_meme_calculator'

    def __init__(self):
        super().__init__(mapped=True)
        self.start = None
        self.count = 0
        self.data_mapping = defaultdict(dict)
        self.posts_score_average = None

    def consume(self, ch, method, properties, body):
        """
        Consumes from the data input queue and process the data
        """
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)
        if message.is_data:
            # If the message is DATA process it, if there's any result forward it to the results
            for i, chunk in enumerate(chunked(self.process_chunk(message.payload), STUDENT_MEME_CHUNKS)):
                meme_list = list(chunk)
                self.send_to_results(meme_list)
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.notify_done()
            end = time()
            print(f"Finished in {end - self.start} seconds")

    def send_to_results(self, payload):
        msg = Message.create_data(payload=payload)
        self.channel.basic_publish(exchange=RESULTS_EXCHANGE,
                                   routing_key=RESULT_STUDENT_MEMES_QUEUE,
                                   body=msg.dump())

    def process_chunk(self, chunk):
        """
        Process Data Chunk

        For each element in the chunk checks if
            - Is a post
            - Is a comment
            - Is the average of all the posts score

        Process each individual data and check if there's any result to return.
        Will yield individual results (generator) to be more memory efficient
        """
        for data in chunk:
            id = data.get('id')
            if id == 'P':
                # Is Posts information
                gen = self.process_post(data)
            elif id == 'C':
                # Is a comment
                gen = self.process_comment(data)
            elif id == 'AVG':
                # It's the posts average
                self.posts_score_average = data['posts_score_average']
                gen = self.process_average()

            for meme in gen:
                yield meme

    def process_post(self, data):
        """
        Process Posts Info

        For each post data received, add it to the data mapping.
        Adds:
            - Post Id
            - Meme URL
            - The Post Score
        If there's info we can return (given we've previously processed comments and AVG result)
        yield the result
        """
        post_id = data['post_id']
        meme_url = data['meme_url']
        post_score = data['post_score']
        comment_data = self.data_mapping[post_id]

        comment_data.update({'meme_url': meme_url, 'post_score': post_score})
        for meme in self.get_student_memes(post_id):
            yield meme

    def process_comment(self, data):
        """
        Process Comments Info

        For each comment data received, add it to the data mapping
        Also checks and yields if there's any result to be returned
        """
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
        """
        Checks if the given post has been liked by students
        """
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
        Process the data stored, considering we've just received the POSTS Score Average
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
    """
    Calculates the Meme with best sentiment
    """
    name = 'sentiment_calculator'

    def __init__(self):
        super().__init__(mapped=False)
        self.start = None
        self.count = 0
        self.data_mapping = defaultdict(dict)
        self.posts_score_average = None

    def consume(self, ch, method, properties, body):
        """
        Consumes from the data input queue and process them
        """
        if not self.start:
            self.start = time()
        raw_data = body
        message = Message.from_bytes(raw_data)

        if message.is_data:
            self.process_chunk(message.payload)
        elif message.eof():
            # We've been informed we will no longer be receiving any data, therefore we must output
            # our information to the next step.
            self.channel.stop_consuming()
            self.process()
            self.notify_done()
            end = time()
            print(f"Finished in {end - self.start} seconds")

    def process_chunk(self, chunk):
        """
        Process Data Chunk

        Each of the elements received can be
            - A Post
            - A Comment
        """
        for data in chunk:
            id = data.get('id')
            if id == 'P':
                # If it is a post add the info to the data mapping
                meme_url = data['meme_url']
                post_id = data['post_id']
                self.data_mapping[post_id].update({'meme_url': meme_url})
            elif id == 'C':
                # If it is a comment add its info to the data mapping
                # Sums to the overall sentiment of the post and increments the comment counter
                # to be used to calculate the average
                post_id = data['post_id']
                sentiment = float(data['sentiment'])
                post_data = self.data_mapping[post_id]
                new_sentiment = post_data.get('sentiment_sum', 0) + sentiment
                new_count = post_data.get('comment_count', 0) + 1

                post_data.update({'sentiment_sum': new_sentiment, 'comment_count': new_count})

    def process(self):
        """
        Processes the Stored data

        For each of the posts in the data mapping
            - First check if we have all the required data (sentiment data and meme url)
            - Calculates the sentiment average of the post
            - Store the post if it is the currently best sentiment post
        """
        best_meme = None
        best_sentiment = None

        for post_id, data in self.data_mapping.items():
            try:
                sentiment_average = data['sentiment_sum'] / data['comment_count']
                meme_url = data['meme_url']
            except (ZeroDivisionError, KeyError):
                # Some data is missing
                # Either we processed comments of a post that wasn't found in the posts set
                # Or a post that didn't have any comments.
                continue
            else:
                if not best_sentiment or sentiment_average > best_sentiment:
                    best_meme = meme_url
                    best_sentiment = sentiment_average
        print(f"The best meme is: {best_meme} with sentiment: {best_sentiment}")
        self.send_result(best_meme)

    def send_result(self, meme):
        msg = Message.create_data(payload=meme)
        self.channel.basic_publish(exchange=RESULTS_EXCHANGE,
                                   routing_key=RESULT_BEST_SENTIMENT_MEME_QUEUE,
                                   body=msg.dump())

