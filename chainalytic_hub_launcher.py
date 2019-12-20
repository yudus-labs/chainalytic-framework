import argparse
from chainalytic.hub import Launcher

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--clean', action='store_true')
    args = parser.parse_args()
    if args.clean:
        Launcher().clean()
    else:
        Launcher().launch()
