import argparse
import sys

from chainalytic.hub import Console

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-s',
        '--sid',
        help='Service ID to init. 0: Upstream, 1: Aggregator, 2: Warehouse, 3: Provider',
    )
    parser.add_argument('-i', '--init-config', action='store_true', help='Generate user config')
    parser.add_argument('--restart', action='store_true', help='Force restart all running services')
    parser.add_argument('-sm', '--skip-monitor', action='store_true', help='Skip monitoring')
    parser.add_argument('-r', '--refresh-time', help='Refresh time of aggregation monitor')

    subparsers = parser.add_subparsers(dest='command')
    clean_parser = subparsers.add_parser('clean', help='Kill running Chainalytic services')
    clean_parser.add_argument(
        'sid', nargs='?', default=None, help='Service ID, kill specific service'
    )

    args = parser.parse_args()
    console = Console()

    try:
        if args.command == 'clean':
            console.load_config()
            console.cleanup_services(args.sid)
        elif args.init_config:
            console.init_config()
        else:
            console.load_config()
            console.init_services(service_id=args.sid, force_restart=args.restart)
            if not args.skip_monitor and not args.sid:
                console.monitor(float(args.refresh_time) if args.refresh_time else 1)
    except KeyboardInterrupt:
        print('Exited Chainalytic Console')
        sys.exit()
