import argparse
import sys
import time

from chainalytic.hub import Console

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-z', '--zone-id', default='public-icon', help='Zone ID to init. Default is "public-icon"',
    )
    parser.add_argument(
        '-s',
        '--sid',
        help='Service ID to init. 0: Upstream, 1: Aggregator, 2: Warehouse, 3: Provider',
    )
    parser.add_argument('-i', '--init-config', action='store_true', help='Generate user config')
    parser.add_argument('--restart', action='store_true', help='Force restart all running services')
    parser.add_argument('--keep-running', action='store_true', help='Prevent console from exiting')

    subparsers = parser.add_subparsers(dest='command', help='Sub commands')
    stop_parser = subparsers.add_parser('stop', help='Kill running Chainalytic services')
    stop_parser.add_argument(
        'sid', nargs='?', default=None, help='Service ID, kill specific service'
    )
    monitor_parser = subparsers.add_parser('m', help='Monitor one specific transform')
    monitor_parser.add_argument(
        'transform_id', nargs='?', default='stake_history', help='Transform ID. Default is "stake_history"',
    )
    monitor_parser.add_argument('-r', '--refresh-time', help='Refresh time of aggregation monitor')

    args = parser.parse_args()
    console = Console()

    try:
        if args.command == 'stop':
            console.load_config()
            console.stop_services(args.sid)
        elif args.command == 'm':
            console.load_config()
            console.monitor(
                args.transform_id, float(args.refresh_time) if args.refresh_time else 1,
            )
        elif args.init_config:
            console.init_config()
        else:
            console.load_config()
            console.init_services(
                args.zone_id, service_id=args.sid, force_restart=args.restart,
            )
            if args.keep_running:
                while 1:
                    time.sleep(999)
    except KeyboardInterrupt:
        print('Exited Chainalytic Console')
        sys.exit()
