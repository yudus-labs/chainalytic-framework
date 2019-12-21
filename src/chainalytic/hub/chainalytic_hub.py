import sys
import os
import subprocess
from subprocess import DEVNULL, STDOUT
from chainalytic.common import config
from chainalytic import (
    aggregator,
    warehouse,
    provider,
)
from chainalytic.common import rpc_client, rpc_server


class ChainalyticHub(object):
    """
    Main hub
    """

    def __init__(self):
        super(ChainalyticHub, self).__init__()
        print('Starting Chainalytic Hub...')
        config.set_working_dir(os.getcwd())
        self.working_dir = config.get_working_dir()
        config.init_user_config(self.working_dir)

        self.upstream_endpoint = config.get_setting()['upstream_endpoint']
        self.aggregator_endpoint = config.get_setting()['aggregator_endpoint']
        self.warehouse_endpoint = config.get_setting()['warehouse_endpoint']
        self.provider_endpoint = config.get_setting()['provider_endpoint']

    def cleanup_services(self):
        for service in [
            self.upstream_endpoint,
            self.aggregator_endpoint,
            self.warehouse_endpoint,
            self.provider_endpoint,
        ]:
            r = rpc_client.call(service, call_id='exit')
            if r['data'] == rpc_server.EXIT_SERVICE:
                print(f'Cleaned service endpoint: {service}')

    def init_services(self):
        self.cleanup_services()
        print('Initializing Chainalytic services...')
        python_exe = sys.executable

        upstream_cmd = [
            python_exe,
            '-m',
            'chainalytic.upstream',
            '--endpoint',
            self.upstream_endpoint,
            '--working_dir',
            os.getcwd(),
        ]
        subprocess.Popen(upstream_cmd, stdout=DEVNULL, stderr=STDOUT)
        print(f'Run Aggregator service: {" ".join(upstream_cmd)}')

        aggregator_cmd = [
            python_exe,
            '-m',
            'chainalytic.aggregator',
            '--endpoint',
            self.aggregator_endpoint,
            '--working_dir',
            os.getcwd(),
        ]
        subprocess.Popen(aggregator_cmd, stdout=DEVNULL, stderr=STDOUT)
        print(f'Run Aggregator service: {" ".join(aggregator_cmd)}')

        warehouse_cmd = [
            python_exe,
            '-m',
            'chainalytic.warehouse',
            '--endpoint',
            self.warehouse_endpoint,
            '--working_dir',
            os.getcwd(),
        ]
        subprocess.Popen(warehouse_cmd, stdout=DEVNULL, stderr=STDOUT)
        print(f'Run Warehouse service: {" ".join(warehouse_cmd)}')

        provider_cmd = [
            python_exe,
            '-m',
            'chainalytic.provider',
            '--endpoint',
            self.provider_endpoint,
            '--working_dir',
            os.getcwd(),
        ]
        subprocess.Popen(provider_cmd, stdout=DEVNULL, stderr=STDOUT)
        print(f'Run Provider service: {" ".join(provider_cmd)}')
        print('Initialized all services')
