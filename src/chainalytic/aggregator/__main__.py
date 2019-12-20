import os
import sys
import argparse
import websockets
import asyncio
import time
from jsonrpcserver import method
from chainalytic.common.rpc_server import EXIT_SERVICE, main_dispatcher
from . import Aggregator

_AGGREGATOR = None


@method
async def _call(call_id: str, **kwargs):
    params = kwargs
    print(f'Call: {call_id}')
    if call_id == 'ping':
        message = ''.join(
            [
                'Pong !\n',
                'Aggregator service is running\n',
                f'Working dir: {_AGGREGATOR.working_dir}\n',
                f'Params: {params}',
            ]
        )
        return message
    elif call_id == 'exit':
        return EXIT_SERVICE
    else:
        return 'Not implemented'


def _run_server(endpoint, working_dir):
    global _AGGREGATOR
    _AGGREGATOR = Aggregator(working_dir)
    print(f'Aggregator endpoint: {endpoint}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])
    start_server = websockets.serve(main_dispatcher, host, port)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
    print('Initialized Aggregator')


# Command to run server, assume you are in root directory of Git repo
# venv/bin/python -m chainalytic.aggregator --endpoint localhost:5500 --working_dir .
#
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Chainalytic Aggregator server')
    parser.add_argument('--endpoint', type=str, help='Endpoint of Aggregator server')
    parser.add_argument('--working_dir', type=str, help='Current working directory')
    args = parser.parse_args()
    endpoint = args.endpoint
    working_dir = args.working_dir if args.working_dir != '.' else os.getcwd()
    _run_server(endpoint, working_dir)
