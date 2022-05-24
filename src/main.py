from argparse import ArgumentParser

from src.common.puppet import PostFilter
from src.common.puppeteer import Puppeteer
from src.posts import PostAvgCalculator, PostFilter
from src.comments import CommentFilter, StudentMemeCalculator
from src.clients import PostClient, CommentClient
import time

ROLES_MAPPER = {
    'puppeteer': Puppeteer,
    'post_filter': PostFilter,
    'post_avg_calculator': PostAvgCalculator,
    'post_filter': PostFilter,
    'post_client': PostClient,
    'comment_filter': CommentFilter,
    'comment_client': CommentClient,
    'student_meme_calculator': StudentMemeCalculator,
}


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-r', '--role', choices=ROLES_MAPPER.keys(), required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    print("arranco el main")
    x = ROLES_MAPPER[args.role]()
    x.main_loop()
    print("termino")

if __name__ == '__main__':
    main()
