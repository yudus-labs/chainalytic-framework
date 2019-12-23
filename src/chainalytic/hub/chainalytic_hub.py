from typing import Optional
import sys
import os
import subprocess
import time
import json
import curses
from subprocess import DEVNULL, STDOUT
from chainalytic.common import rpc_client, rpc_server, config
from jsonrpcclient.clients.http_client import HTTPClient


class ChainalyticHub(object):
    """
    Main hub

    Properties:
        working_dir (str):
        upstream_endpoint (str):
        aggregator_endpoint (str):
        warehouse_endpoint (str):
        provider_endpoint (str):

    Methods:
        cleanup_services()
        init_services()
        monitor()

    """

    def __init__(self, working_dir: Optional[str] = None):
        super(ChainalyticHub, self).__init__()
        print('Starting Chainalytic Hub...')

        if not working_dir:
            config.set_working_dir(os.getcwd())
        else:
            config.set_working_dir(working_dir)
        self.working_dir = config.get_working_dir()
        config.init_user_config(self.working_dir)

        self.upstream_endpoint = config.get_setting()['upstream_endpoint']
        self.aggregator_endpoint = config.get_setting()['aggregator_endpoint']
        self.warehouse_endpoint = config.get_setting()['warehouse_endpoint']
        self.provider_endpoint = config.get_setting()['provider_endpoint']

    def cleanup_services(self):
        for service in [
            self.provider_endpoint,
            self.aggregator_endpoint,
            self.upstream_endpoint,
            self.warehouse_endpoint,
        ]:
            r = rpc_client.call(service, call_id='exit')
            if r['data'] == rpc_server.EXIT_SERVICE:
                print(f'Cleaned service endpoint: {service}')

    def init_services(self, force_restart: bool = 0):
        if force_restart:
            self.cleanup_services()
        print('Initializing Chainalytic services...')
        python_exe = sys.executable

        if not rpc_client.call(self.upstream_endpoint, call_id='ping')['status']:
            upstream_cmd = [
                python_exe,
                '-m',
                'chainalytic.upstream',
                '--endpoint',
                self.upstream_endpoint,
                '--zone_id',
                'public-icon',
                '--working_dir',
                os.getcwd(),
            ]
            subprocess.Popen(upstream_cmd, stdout=DEVNULL, stderr=STDOUT)
            print(f'Started Aggregator service: {" ".join(upstream_cmd)}')
            print()

        if not rpc_client.call(self.warehouse_endpoint, call_id='ping')['status']:
            warehouse_cmd = [
                python_exe,
                '-m',
                'chainalytic.warehouse',
                '--endpoint',
                self.warehouse_endpoint,
                '--zone_id',
                'public-icon',
                '--working_dir',
                os.getcwd(),
            ]
            subprocess.Popen(warehouse_cmd, stdout=DEVNULL, stderr=STDOUT)
            print(f'Started Warehouse service: {" ".join(warehouse_cmd)}')
            print()

        if not rpc_client.call(self.aggregator_endpoint, call_id='ping')['status']:
            aggregator_cmd = [
                python_exe,
                '-m',
                'chainalytic.aggregator',
                '--endpoint',
                self.aggregator_endpoint,
                '--zone_id',
                'public-icon',
                '--working_dir',
                os.getcwd(),
            ]
            subprocess.Popen(aggregator_cmd, stdout=DEVNULL, stderr=STDOUT)
            print(f'Started Aggregator service: {" ".join(aggregator_cmd)}')
            print()

        if not rpc_client.call(self.provider_endpoint, call_id='ping')['status']:
            provider_cmd = [
                python_exe,
                '-m',
                'chainalytic.provider',
                '--endpoint',
                self.provider_endpoint,
                '--zone_id',
                'public-icon',
                '--working_dir',
                os.getcwd(),
            ]
            subprocess.Popen(provider_cmd, stdout=DEVNULL, stderr=STDOUT)
            print(f'Started Provider service: {" ".join(provider_cmd)}')
            print()
            print('Initialized all services')
            print()

    def monitor(self):
        client = HTTPClient(f'http://{self.provider_endpoint}')

        stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()

        try:
            while 1:
                response = client.request(
                    "_call",
                    call_id='api_call',
                    api_id='last_block_height',
                    api_params={'transform_id': 'stake_history'},
                )
                r1 = response.data.result

                response = client.request(
                    "_call",
                    call_id='api_call',
                    api_id='get_staking_info_last_block',
                    api_params={'transform_id': 'stake_history'},
                )
                r2 = json.loads(response.data.result['result'])

                if r1 and r2:
                    total_staking = round(r2['total_staking'], 2)
                    total_unstaking = round(r2['total_unstaking'], 2)
                    total_staking_wallets = round(r2['total_staking_wallets'], 2)

                    stdscr.addstr(0, 0, '== Data Aggregation Monitor ==')
                    stdscr.addstr(1, 0, f'Upstream service: {self.upstream_endpoint}')
                    stdscr.addstr(2, 0, f'Aggregator service: {self.aggregator_endpoint}')
                    stdscr.addstr(3, 0, f'Warehouse service: {self.warehouse_endpoint}')
                    stdscr.addstr(4, 0, f'Provider service: {self.provider_endpoint}')
                    stdscr.addstr(5, 0, f'Working dir: {self.working_dir}')
                    stdscr.addstr(6, 0, f'----')
                    stdscr.addstr(7, 0, f'Last block: {r1["result"]:,}')
                    stdscr.addstr(8, 0, f'Total staking: {total_staking:,}')
                    stdscr.addstr(9, 0, f'Total unstaking: {total_unstaking:,}')
                    stdscr.addstr(10, 0, f'Total staking wallets: {total_staking_wallets:,}')
                    stdscr.refresh()

                time.sleep(1)
        finally:
            curses.echo()
            curses.nocbreak()
            curses.endwin()
