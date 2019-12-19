import sys
import os
import subprocess
from chainalytic.common import config
from chainalytic import (
    aggregator,
    warehouse,
    provider,
)


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

        self.aggregator_endpoint = config.get_setting()['aggregator_endpoint']
        self.warehouse_endpoint = config.get_setting()['warehouse_endpoint']
        self.provider_endpoint = config.get_setting()['provider_endpoint']

    def init_services(self):
        print('Initializing Chainalytic services...')
        python_exe = sys.executable

        aggregator_cmd = [
            python_exe,
            '-m',
            'chainalytic.aggregator',
            '--endpoint',
            self.aggregator_endpoint,
            '--working_dir',
            os.getcwd(),
        ]
        subprocess.Popen(aggregator_cmd)
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
        subprocess.Popen(warehouse_cmd)
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
        subprocess.Popen(provider_cmd)
        print(f'Run Provider service: {" ".join(provider_cmd)}')
        print('Initialized all services')
