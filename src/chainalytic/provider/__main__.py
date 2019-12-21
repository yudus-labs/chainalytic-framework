"""
Sample service calls

from chainalytic.common import rpc_client
rpc_client.call(
    f'localhost:5530',
    call_id='api_call',
    api_id='get_unstaking',
    api_params={'height': 999}
)

"""

import os
import argparse
import websockets
import asyncio
import time
from jsonrpcserver import method
from chainalytic.common.rpc_server import EXIT_SERVICE, main_dispatcher
from . import Provider

_PROVIDER = None


@method
async def _call(call_id: str, **kwargs):
    params = kwargs
    print(f'Call: {call_id}')
    if call_id == 'ping':
        message = ''.join(
            [
                'Pong !\n',
                'Provider service is running\n',
                f'Working dir: {_PROVIDER.working_dir}\n',
                f'Params: {params}',
            ]
        )
        return message
    elif call_id == 'exit':
        return EXIT_SERVICE
    elif call_id == 'api_call':
        api_id = params['api_id']
        api_params = params['api_params']
        return await _PROVIDER.api_bundle.api_call(api_id, api_params)
    else:
        return f'Not implemented'


def _run_server(endpoint, working_dir, zone_id):
    global _PROVIDER
    _PROVIDER = Provider(working_dir, zone_id)
    print(f'Provider endpoint: {endpoint}')
    print(f'Provider zone ID: {zone_id}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])
    start_server = websockets.serve(main_dispatcher, host, port)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
    print('Initialized Provider')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Chainalytic Provider server')
    parser.add_argument('--endpoint', type=str, help='Endpoint of Provider server')
    parser.add_argument('--working_dir', type=str, help='Current working directory')
    parser.add_argument('--zone_id', type=str, help='Zone ID')
    args = parser.parse_args()
    endpoint = args.endpoint
    working_dir = args.working_dir if args.working_dir != '.' else os.getcwd()
    zone_id = args.zone_id
    _run_server(endpoint, working_dir, zone_id)
