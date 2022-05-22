from argparse import ArgumentParser
from src.orchestrator import main as orchestrator
from src.mapper import main as mapper

from src.common.puppet import PostFilter
from src.common.puppeteer import Puppeteer
import time


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-r', '--role', choices=('server', 'client'), required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    print("arranco el main")
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
if __name__ == '__main__':
    main()
