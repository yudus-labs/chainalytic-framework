import curses
import functools
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pprint import pprint
from subprocess import DEVNULL, PIPE, STDOUT
from typing import Optional

from jsonrpcclient.clients.http_client import HTTPClient

from chainalytic.common import config, rpc_client, rpc_server

C = 'CONNECTED'
D = 'DISCONNECTED'


def seconds_to_datetime(seconds: int):
    sec = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + sec
    return f'{d.day - 1}d {d.hour}h {d.minute}m {d.second}s'


def handle_curses_break(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        finally:
            curses.echo()
            curses.nocbreak()
            curses.endwin()

    return wrapper


class Console(object):
    """
    Main console for managing Chainalytic services

    Service IDs:
        0: Upstream
        1: Aggregator
        2: Warehouse
        3: Provider

    Properties:
        working_dir (str):
        cfg (dict):
        upstream_endpoint (str):
        aggregator_endpoint (str):
        warehouse_endpoint (str):
        provider_endpoint (str):
        is_endpoint_set (bool):

    Methods:
        stop_services()
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

    @property
    def sid(self):
        sid_info = {
            '0': {
                'endpoint': self.upstream_endpoint,
                'name': 'Upstream',
                'py_pkg': 'chainalytic.upstream',
            },
            '1': {
                'endpoint': self.aggregator_endpoint,
                'name': 'Aggregator',
                'py_pkg': 'chainalytic.aggregator',
            },
            '2': {
                'endpoint': self.warehouse_endpoint,
                'name': 'Warehouse',
                'py_pkg': 'chainalytic.warehouse',
            },
            '3': {
                'endpoint': self.provider_endpoint,
                'name': 'Provider',
                'py_pkg': 'chainalytic.provider',
            },
        }
        return sid_info

    def stop_services(self, service_id: Optional[str] = None):
        assert self.is_endpoint_set, 'Service endpoints are not set, please load config first'

        print('Stopping services...')
        cleaned = 0

        if service_id:
            all_sid = [service_id] if service_id in self.sid else []
        else:
            all_sid = self.sid

        for i in all_sid:
            if i == '3':
                r = rpc_client.call_aiohttp(self.sid[i]['endpoint'], call_id='ping')
                if r['status']:
                    rpc_client.call_aiohttp(self.sid[i]['endpoint'], call_id='exit')
            else:
                r = rpc_client.call(self.sid[i]['endpoint'], call_id='exit')

            if r['status']:
                cleaned = 1
                print(
                    f'----Stopped {self.sid[i]["name"]} service endpoint: {self.sid[i]["endpoint"]}'
                )

        print('Stopped all Chainalytic services' if cleaned else 'Nothing to stop')

    def init_services(
        self, zone_id: str, service_id: Optional[str] = None, force_restart: Optional[bool] = 0
    ):
        assert self.is_endpoint_set, 'Service endpoints are not set, please load config first'

        if force_restart:
            self.stop_services()

        print('Initializing Chainalytic services...')
        print('')
        python_exe = sys.executable

        if service_id:
            all_sid = [service_id] if service_id in self.sid else []
            if not all_sid:
                raise Exception('Invalid service ID')
        else:
            all_sid = self.sid

        sudo = ['sudo', '-S'] if 'SUDO_PWD' in os.environ else []
        echo_pwd = ['echo', os.environ['SUDO_PWD']] if 'SUDO_PWD' in os.environ else []

        for i in all_sid:
            if i == '3':
                pong = rpc_client.call_aiohttp(self.sid[i]['endpoint'], call_id='ping')['status']
            else:
                pong = rpc_client.call(self.sid[i]['endpoint'], call_id='ping')['status']
            if not pong:
                echo = subprocess.Popen(echo_pwd, stdout=PIPE) if echo_pwd else None
                cmd = sudo + [
                    python_exe,
                    '-m',
                    self.sid[i]['py_pkg'],
                    '--endpoint',
                    self.sid[i]['endpoint'],
                    '--zone_id',
                    zone_id,
                    '--working_dir',
                    os.getcwd(),
                ]
                subprocess.Popen(
                    cmd,
                    stdin=echo.stdout if echo else None,
                    stdout=DEVNULL,
                    stderr=STDOUT,
                    start_new_session=True,
                )
                print(f'----Started {self.sid[i]["name"]} service: {" ".join(cmd)}')
                print('')

        if not all_sid:
            print('No service initialized')
        print('')

    @handle_curses_break
    def monitor_stake_history(
        self,
        zone_id: str,
        transform_id: str,
        provider_client: HTTPClient,
        stdscr: '_curses.window',
        refresh_time: float,
    ):

        latest_block_height = 0
        prev_last_block = 0
        prev_time = time.time()

        last_block = 0
        total_staking = 0
        total_unstaking = 0
        total_staking_wallets = 0
        total_unstaking_wallets = 0

        while 1:
            upstream_connected = rpc_client.call(self.upstream_endpoint, call_id='ping')['status']
            aggregator_connected = rpc_client.call(self.aggregator_endpoint, call_id='ping')[
                'status'
            ]
            warehouse_connected = rpc_client.call(self.warehouse_endpoint, call_id='ping')['status']
            provider_connected = provider_client.request("_call", call_id='ping').data.result

            if upstream_connected:
                upstream_response = rpc_client.call(
                    self.upstream_endpoint, call_id='last_block_height'
                )
                if upstream_response['status']:
                    latest_block_height = upstream_response['data']

            if (
                upstream_connected
                and aggregator_connected
                and warehouse_connected
                and provider_connected
            ):
                r = provider_client.request(
                    "_call",
                    call_id='api_call',
                    api_id='get_staking_info_last_block',
                    api_params={'transform_id': transform_id},
                ).data.result['result']
                if r:
                    last_block = r['height']
                    total_staking = round(r['total_staking'], 2)
                    total_unstaking = round(r['total_unstaking'], 2)
                    total_staking_wallets = round(r['total_staking_wallets'], 2)
                    total_unstaking_wallets = round(r['total_unstaking_wallets'], 2)

            speed = int((last_block - prev_last_block) / (time.time() - prev_time))
            prev_last_block = last_block
            prev_time = time.time()

            if latest_block_height > last_block > 0 and speed > 0:
                remaining_time = seconds_to_datetime((latest_block_height - last_block) / speed)
            elif latest_block_height == last_block and last_block > 0:
                remaining_time = 'Fully synced'
            elif latest_block_height < last_block:
                remaining_time = (
                    'Upstream block height is lower than latest aggregated block (out-of-date)'
                )
            else:
                remaining_time = 'N/A'

            remaining_blocks = abs(latest_block_height - last_block)

            c1 = C if upstream_connected else D
            c2 = C if aggregator_connected else D
            c3 = C if warehouse_connected else D
            c4 = C if provider_connected else D

            stdscr.erase()
            stdscr.addstr(
                0,
                0,
                f'== Data Aggregation Monitor | Zone ID: {zone_id} | Transform ID: {transform_id} ==',
            )
            stdscr.addstr(1, 0, f'Upstream service:   {self.upstream_endpoint} {c1}')
            stdscr.addstr(2, 0, f'Aggregator service: {self.aggregator_endpoint} {c2}')
            stdscr.addstr(3, 0, f'Warehouse service:  {self.warehouse_endpoint} {c3}')
            stdscr.addstr(4, 0, f'Provider service:   {self.provider_endpoint} {c4}')
            stdscr.addstr(5, 0, f'Working dir: {self.working_dir}')
            stdscr.addstr(6, 0, f'----')
            stdscr.addstr(7, 0, f'Latest upstream block:    {latest_block_height:,}')
            stdscr.addstr(8, 0, f'Latest aggregated block:  {last_block:,}')
            stdscr.addstr(9, 0, f'Data aggregation speed:   {speed} blocks/s')
            stdscr.addstr(10, 0, f'Remaining blocks:         {remaining_blocks:,}')
            stdscr.addstr(11, 0, f'Estimated time remaining: {remaining_time}')
            stdscr.addstr(12, 0, f'----')
            stdscr.addstr(13, 0, f'Total staking:           {total_staking:,}')
            stdscr.addstr(14, 0, f'Total unstaking:         {total_unstaking:,}')
            stdscr.addstr(15, 0, f'Total staking wallets:   {total_staking_wallets:,}')
            stdscr.addstr(16, 0, f'Total unstaking wallets: {total_unstaking_wallets:,}')
            stdscr.refresh()

            time.sleep(refresh_time)

    @handle_curses_break
    def monitor_basic(
        self,
        zone_id: str,
        transform_id: str,
        provider_client: HTTPClient,
        stdscr: '_curses.window',
        refresh_time: float,
    ):

        latest_block_height = 0
        prev_last_block = 0
        prev_time = time.time()

        last_block = 0

        while 1:
            upstream_connected = rpc_client.call(self.upstream_endpoint, call_id='ping')['status']
            aggregator_connected = rpc_client.call(self.aggregator_endpoint, call_id='ping')[
                'status'
            ]
            warehouse_connected = rpc_client.call(self.warehouse_endpoint, call_id='ping')['status']
            provider_connected = provider_client.request("_call", call_id='ping').data.result

            if upstream_connected:
                upstream_response = rpc_client.call(
                    self.upstream_endpoint, call_id='last_block_height'
                )
                if upstream_response['status']:
                    latest_block_height = upstream_response['data']

            if (
                upstream_connected
                and aggregator_connected
                and warehouse_connected
                and provider_connected
            ):
                r = provider_client.request(
                    "_call",
                    call_id='api_call',
                    api_id='last_block_height',
                    api_params={'transform_id': transform_id},
                ).data.result['result']
                if r:
                    last_block = r

            speed = int((last_block - prev_last_block) / (time.time() - prev_time))
            prev_last_block = last_block
            prev_time = time.time()

            if latest_block_height > last_block > 0 and speed > 0:
                remaining_time = seconds_to_datetime((latest_block_height - last_block) / speed)
            elif latest_block_height == last_block and last_block > 0:
                remaining_time = 'Fully synced'
            elif latest_block_height < last_block:
                remaining_time = (
                    'Upstream block height is lower than latest aggregated block (out-of-date)'
                )
            else:
                remaining_time = 'N/A'

            remaining_blocks = abs(latest_block_height - last_block)

            c1 = C if upstream_connected else D
            c2 = C if aggregator_connected else D
            c3 = C if warehouse_connected else D
            c4 = C if provider_connected else D

            stdscr.erase()
            stdscr.addstr(
                0,
                0,
                f'== Data Aggregation Monitor | Zone ID: {zone_id} | Transform ID: {transform_id} ==',
            )
            stdscr.addstr(1, 0, f'Upstream service:   {self.upstream_endpoint} {c1}')
            stdscr.addstr(2, 0, f'Aggregator service: {self.aggregator_endpoint} {c2}')
            stdscr.addstr(3, 0, f'Warehouse service:  {self.warehouse_endpoint} {c3}')
            stdscr.addstr(4, 0, f'Provider service:   {self.provider_endpoint} {c4}')
            stdscr.addstr(5, 0, f'Working dir: {self.working_dir}')
            stdscr.addstr(6, 0, f'----')
            stdscr.addstr(7, 0, f'Latest upstream block:    {latest_block_height:,}')
            stdscr.addstr(8, 0, f'Latest aggregated block:  {last_block:,}')
            stdscr.addstr(9, 0, f'Data aggregation speed:   {speed} blocks/s')
            stdscr.addstr(10, 0, f'Remaining blocks:         {remaining_blocks:,}')
            stdscr.addstr(11, 0, f'Estimated time remaining: {remaining_time}')
            stdscr.refresh()

            time.sleep(refresh_time)

    @handle_curses_break
    def monitor_all(
        self,
        zone_id: str,
        all_transform_ids: list,
        provider_client: HTTPClient,
        stdscr: '_curses.window',
        refresh_time: float,
    ):
        all_transforms_last_block = {tid: 0 for tid in all_transform_ids}
        all_transforms_prev_last_block = {tid: 0 for tid in all_transform_ids}
        all_transforms_prev_time = {tid: 0 for tid in all_transform_ids}
        all_transforms_speed = {tid: 0 for tid in all_transform_ids}
        latest_block_height = 0

        while 1:
            upstream_connected = rpc_client.call(self.upstream_endpoint, call_id='ping')['status']
            aggregator_connected = rpc_client.call(self.aggregator_endpoint, call_id='ping')[
                'status'
            ]
            warehouse_connected = rpc_client.call(self.warehouse_endpoint, call_id='ping')['status']
            provider_connected = provider_client.request("_call", call_id='ping').data.result

            if upstream_connected:
                upstream_response = rpc_client.call(
                    self.upstream_endpoint, call_id='last_block_height'
                )
                if upstream_response['status']:
                    latest_block_height = upstream_response['data']

            if (
                upstream_connected
                and aggregator_connected
                and warehouse_connected
                and provider_connected
            ):
                for tid in all_transform_ids:
                    last_block = provider_client.request(
                        "_call",
                        call_id='api_call',
                        api_id='last_block_height',
                        api_params={'transform_id': tid},
                    ).data.result['result']
                    if last_block:
                        all_transforms_prev_last_block[tid] = all_transforms_last_block[tid]
                        all_transforms_last_block[tid] = last_block
                        all_transforms_speed[tid] = int(
                            (last_block - all_transforms_prev_last_block[tid])
                            / (time.time() - all_transforms_prev_time[tid])
                        )
                        all_transforms_prev_time[tid] = time.time()

            c1 = C if upstream_connected else D
            c2 = C if aggregator_connected else D
            c3 = C if warehouse_connected else D
            c4 = C if provider_connected else D

            stdscr.erase()
            stdscr.addstr(
                0, 0, f'== Data Aggregation Monitor | Zone ID: {zone_id} | All Transforms ==',
            )
            stdscr.addstr(1, 0, f'Upstream service:   {self.upstream_endpoint} {c1}')
            stdscr.addstr(2, 0, f'Aggregator service: {self.aggregator_endpoint} {c2}')
            stdscr.addstr(3, 0, f'Warehouse service:  {self.warehouse_endpoint} {c3}')
            stdscr.addstr(4, 0, f'Provider service:   {self.provider_endpoint} {c4}')
            stdscr.addstr(5, 0, f'Working dir: {self.working_dir}')
            stdscr.addstr(6, 0, f'----')
            stdscr.addstr(7, 0, f'Latest upstream block:  {latest_block_height:,}')
            stdscr.addstr(8, 0, f'Latest aggregated block of all transforms')
            for i, tid in enumerate(all_transforms_last_block):
                stdscr.addstr(
                    9 + i,
                    0,
                    f'----{tid}:  {all_transforms_last_block[tid]:,} | {all_transforms_speed[tid]} blocks/s',
                )

            stdscr.refresh()

            time.sleep(refresh_time)

    def monitor(self, transform_id: Optional[str], refresh_time: float = 1.0):
        assert self.is_endpoint_set, 'Service endpoints are not set, please load config first'

        print('Starting aggregation monitor, waiting for Provider and Aggregator service...')
        r1 = rpc_client.call_aiohttp(self.provider_endpoint, call_id='ping')['status']
        r2 = rpc_client.call(self.aggregator_endpoint, call_id='ping')['status']
        while not (r1 and r2):
            time.sleep(0.1)
            r1 = rpc_client.call_aiohttp(self.provider_endpoint, call_id='ping')['status']
            r2 = rpc_client.call(self.aggregator_endpoint, call_id='ping')['status']

        provider_client = HTTPClient(f'http://{self.provider_endpoint}')
        zone_id = rpc_client.call_aiohttp(self.provider_endpoint, call_id='get_zone_id')['data']

        stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()

        r = rpc_client.call(self.aggregator_endpoint, call_id='ls_all_transform_id')
        if r['status']:
            all_transforms = r['data']
        else:
            curses.echo()
            curses.nocbreak()
            curses.endwin()
            print('Cannot query official transform IDs, exited console')
            return

        if not transform_id:
            self.monitor_all(zone_id, all_transforms, provider_client, stdscr, refresh_time)
        elif transform_id == 'stake_history' and transform_id in all_transforms:
            self.monitor_stake_history(zone_id, transform_id, provider_client, stdscr, refresh_time)
        elif transform_id in all_transforms:
            self.monitor_basic(zone_id, transform_id, provider_client, stdscr, refresh_time)
        else:
            curses.echo()
            curses.nocbreak()
            curses.endwin()
            print(f'Transform "{transform_id}" not found, exited console')
