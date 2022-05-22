from argparse import ArgumentParser
from src.orchestrator import main as orchestrator
from src.mapper import main as mapper

from src.common.puppet import PostFilter
from src.common.puppeteer import Puppeteer
from src.posts import PostAvgCalculator, PostFilter
import time

ROLES_MAPPER = {
    'puppeteer': Puppeteer,
    'post_filter': PostFilter,
    'post_avg_calculator': PostAvgCalculator,
    'post_filter': PostFilter,
}


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-r', '--role', choices=ROLES_MAPPER.keys(), required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    print("arranco el main")
    x = ROLES_MAPPER[args.role]()
    x.connect()
    x.init()
    x.run()
    """
    if args.role == 'server':
        print("Arranco el orquestador")
        #orchestrator()
        x = Puppeteer()
        x.connect()
        x.init()
        x.run()
    else:
        print("arranco un mapper")
        #mapper()
        x = PostFilter()
        x.connect()
        x.init()
        x.run()
    """
if __name__ == '__main__':
    main()
