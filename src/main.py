from argparse import ArgumentParser

from src.common.puppeteer import Puppeteer
from src.posts import PostAvgCalculator, PostFilter
from src.comments import CommentFilter, StudentMemeCalculator, SentimentMeme
from src.clients import PostClient, CommentClient, ResultClient

ROLES_MAPPER = {
    'puppeteer': Puppeteer,
    'post_filter': PostFilter,
    'post_avg_calculator': PostAvgCalculator,
    'post_filter': PostFilter,
    'post_client': PostClient,
    'comment_filter': CommentFilter,
    'comment_client': CommentClient,
    'student_meme_calculator': StudentMemeCalculator,
    'sentiment_meme_calculator': SentimentMeme,
    'result_client': ResultClient,
}


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-r', '--role', choices=ROLES_MAPPER.keys(), required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    print("Starting")
    x = ROLES_MAPPER[args.role]()
    x.main_loop()
    print("Finalizing")

if __name__ == '__main__':
    main()
