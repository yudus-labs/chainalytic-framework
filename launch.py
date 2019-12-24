import argparse
from chainalytic.hub import Console

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--clean', action='store_true', help='Kill all running services')
    parser.add_argument('--endpoint', help='Kill specific service, works with --clean')
    parser.add_argument('--restart', action='store_true', help='Force restart all running services')
    parser.add_argument('--skip-monitor', action='store_true', help='Skip monitoring')
    parser.add_argument('-r', '--refresh-time', help='Refresh time of aggregation monitor')
    args = parser.parse_args()
    console = Console()

    if args.clean:
        console.cleanup_services(args.endpoint if args.endpoint else None)
    else:
        console.init_services(force_restart=args.restart)
        if not args.skip_monitor:
            console.monitor(float(args.refresh_time) if args.refresh_time else 1)

