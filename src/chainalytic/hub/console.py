import curses
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pprint import pprint
from subprocess import DEVNULL, STDOUT
from typing import Optional

from jsonrpcclient.clients.http_client import HTTPClient

from chainalytic.common import config, rpc_client, rpc_server

C = 'CONNECTED'
D = 'DISCONNECTED'


def seconds_to_datetime(seconds: int):
    sec = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + sec
    return f'{d.day - 1} days, {d.hour} hours, {d.minute} minutes, {d.second} seconds'


class Console(object):
    """
    Main console for managing Chainalytic services

    Properties:
        working_dir (str):
        upstream_endpoint (str):
        aggregator_endpoint (str):
        warehouse_endpoint (str):
        provider_endpoint (str):
        is_endpoint_set (bool):

    Methods:
        cleanup_services()
        init_services()
        monitor()

    """

    def __init__(self, working_dir: Optional[str] = None):
        super(Console, self).__init__()
        print('Starting Chainalytic Console...')

        self.working_dir = working_dir if working_dir else os.getcwd()
        self.cfg = None
        self.upstream_endpoint = None
        self.aggregator_endpoint = None
        self.warehouse_endpoint = None
        self.provider_endpoint = None

    def init_config(self):
        """Load user config ( and generate it if not found )"""
        cfg = config.init_user_config(self.working_dir)
        if not cfg:
            raise Exception('Failed to init user config')

        print('Generated user config')
        print(f'--Chain registry: {cfg["chain_registry"]}')
        print(f'--Setting: {cfg["setting"]}')

    def load_config(self):
        if not config.check_user_config(self.working_dir):
            raise Exception('User config not found, please init config first')

        config.set_working_dir(self.working_dir)
        self.upstream_endpoint = config.get_setting()['upstream_endpoint']
        self.aggregator_endpoint = config.get_setting()['aggregator_endpoint']
        self.warehouse_endpoint = config.get_setting()['warehouse_endpoint']
        self.provider_endpoint = config.get_setting()['provider_endpoint']

    @property
    def is_endpoint_set(self) -> bool:
        return (
            self.upstream_endpoint
            and self.aggregator_endpoint
            and self.warehouse_endpoint
            and self.provider_endpoint
        )

    def cleanup_services(self, endpoint: str = None):
        assert self.is_endpoint_set, 'Service endpoints are not set, please load config first'

        print('Cleaning up services...')
        cleaned = 0
        if endpoint:
            all_endpoints = [endpoint]
        else:
            all_endpoints = [
                self.provider_endpoint,
                self.aggregator_endpoint,
                self.upstream_endpoint,
                self.warehouse_endpoint,
            ]

        for service in all_endpoints:
            if service == self.provider_endpoint:
                r = rpc_client.call_aiohttp(service, call_id='ping')
                if r['status']:
                    rpc_client.call_aiohttp(service, call_id='exit')
            else:
                r = rpc_client.call(service, call_id='exit')

            if r['status']:
                cleaned = 1
                print(f'----Cleaned service endpoint: {service}')

        print('Cleaned all Chainalytic services' if cleaned else 'Nothing to clean')

    def init_services(self, force_restart: bool = 0):
        assert self.is_endpoint_set, 'Service endpoints are not set, please load config first'

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
            subprocess.Popen(upstream_cmd, stdout=DEVNULL, stderr=STDOUT, start_new_session=True)
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
            subprocess.Popen(warehouse_cmd, stdout=DEVNULL, stderr=STDOUT, start_new_session=True)
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
            subprocess.Popen(aggregator_cmd, stdout=DEVNULL, stderr=STDOUT, start_new_session=True)
            print(f'Started Aggregator service: {" ".join(aggregator_cmd)}')
            print()

        if not rpc_client.call_aiohttp(self.provider_endpoint, call_id='ping')['status']:
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
            subprocess.Popen(provider_cmd, stdout=DEVNULL, stderr=STDOUT, start_new_session=True)
            print(f'Started Provider service: {" ".join(provider_cmd)}')
            print()
        print('Initialized all services')
        print()

    def monitor(self, refresh_time: float = 1.0):
        assert self.is_endpoint_set, 'Service endpoints are not set, please load config first'

        print('Starting aggregation monitor, waiting for Provider service...')
        ready = rpc_client.call_aiohttp(self.provider_endpoint, call_id='ping')['status']
        while not ready:
            time.sleep(0.1)
            ready = rpc_client.call_aiohttp(self.provider_endpoint, call_id='ping')['status']

        client = HTTPClient(f'http://{self.provider_endpoint}')

        stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()

        try:
            latest_block_height = 0
            prev_last_block = 0
            prev_time = time.time()
            while 1:
                upstream_connected = rpc_client.call(self.upstream_endpoint, call_id='ping')[
                    'status'
                ]
                aggregator_connected = rpc_client.call(self.aggregator_endpoint, call_id='ping')[
                    'status'
                ]
                warehouse_connected = rpc_client.call(self.warehouse_endpoint, call_id='ping')[
                    'status'
                ]
                provider_connected = client.request("_call", call_id='ping',).data.result

                if upstream_connected:
                    upstream_response = rpc_client.call(
                        self.upstream_endpoint, call_id='last_block_height'
                    )
                    if upstream_response['status']:
                        latest_block_height = upstream_response['data']

                last_block = 0
                total_staking = 0
                total_unstaking = 0
                total_staking_wallets = 0

                if (
                    upstream_connected
                    and aggregator_connected
                    and warehouse_connected
                    and provider_connected
                ):
                    res = client.request(
                        "_call",
                        call_id='api_call',
                        api_id='last_block_height',
                        api_params={'transform_id': 'stake_history'},
                    )
                    r1 = res.data.result

                    res = client.request(
                        "_call",
                        call_id='api_call',
                        api_id='get_staking_info_last_block',
                        api_params={'transform_id': 'stake_history'},
                    )
                    if res.data.result['result']:
                        r2 = json.loads(res.data.result['result'])

                        last_block = r1["result"]
                        total_staking = round(r2['total_staking'], 2)
                        total_unstaking = round(r2['total_unstaking'], 2)
                        total_staking_wallets = round(r2['total_staking_wallets'], 2)

                speed = int((last_block - prev_last_block) / (time.time() - prev_time))
                prev_last_block = last_block
                prev_time = time.time()

                if latest_block_height > 0 and speed > 0:
                    remaining_time = seconds_to_datetime((latest_block_height - last_block) / speed)
                elif latest_block_height == last_block and last_block > 0:
                    remaining_time = 'Fully synced'
                else:
                    remaining_time = 'N/A'

                c1 = C if upstream_connected else D
                c2 = C if aggregator_connected else D
                c3 = C if warehouse_connected else D
                c4 = C if provider_connected else D

                stdscr.erase()
                stdscr.addstr(0, 0, '== Data Aggregation Monitor ==')
                stdscr.addstr(1, 0, f'Upstream service:   {self.upstream_endpoint} {c1}')
                stdscr.addstr(2, 0, f'Aggregator service: {self.aggregator_endpoint} {c2}')
                stdscr.addstr(3, 0, f'Warehouse service:  {self.warehouse_endpoint} {c3}')
                stdscr.addstr(4, 0, f'Provider service:   {self.provider_endpoint} {c4}')
                stdscr.addstr(5, 0, f'Working dir: {self.working_dir}')
                stdscr.addstr(6, 0, f'----')
                stdscr.addstr(7, 0, f'Latest block upstream chain: {latest_block_height:,}')
                stdscr.addstr(8, 0, f'Latest aggregated block:     {last_block:,}')
                stdscr.addstr(9, 0, f'Data aggregation speed:      {speed} blocks/s')
                stdscr.addstr(10, 0, f'Estimated time remaining:    {remaining_time}')
                stdscr.addstr(11, 0, f'----')
                stdscr.addstr(12, 0, f'Total staking:         {total_staking:,}')
                stdscr.addstr(13, 0, f'Total unstaking:       {total_unstaking:,}')
                stdscr.addstr(14, 0, f'Total staking wallets: {total_staking_wallets:,}')
                stdscr.refresh()

                time.sleep(refresh_time)
        finally:
            curses.echo()
            curses.nocbreak()
            curses.endwin()
