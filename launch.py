import argparse
from chainalytic.hub import Console

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--clean', action='store_true')
    parser.add_argument('--restart', action='store_true')
    parser.add_argument('-r', '--refresh-time')
    args = parser.parse_args()
    console = Console()

    if args.clean:
        console.cleanup_services()
    else:
        console.init_services(force_restart=args.restart)
        console.monitor(float(args.refresh_time) if args.refresh_time else 1)

