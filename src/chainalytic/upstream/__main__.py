"""
Sample service calls

from chainalytic.common import rpc_client
rpc_client.call('localhost:5500', call_id='get_block', height=10000000)

"""
import os
import argparse
import websockets
import asyncio
import time
from jsonrpcserver import method
from chainalytic.common.rpc_server import EXIT_SERVICE, main_dispatcher, show_call_info
from . import Upstream

_UPSTREAM = None


@method
async def _call(call_id: str, **kwargs):
    params = kwargs
    show_call_info(call_id, params)

    if call_id == 'ping':
        message = '\n'.join(
            [
                'Pong !',
                'Upstream service is running',
                f'Working dir: {_UPSTREAM.working_dir}',
                f'Params: {params}',
            ]
        )
        return message
    elif call_id == 'exit':
        return EXIT_SERVICE
    elif call_id == 'get_block':
        height = params['height']
        return await _UPSTREAM.data_feeder.get_block(height)
    else:
        return f'Not implemented'


def _run_server(endpoint, working_dir, zone_id):
    global _UPSTREAM
    _UPSTREAM = Upstream(working_dir, zone_id)
    print(f'Upstream endpoint: {endpoint}')
    print(f'Upstream zone ID: {zone_id}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])
    start_server = websockets.serve(main_dispatcher, host, port)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
    print('Exited Upstream')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Chainalytic Upstream server')
    parser.add_argument('--endpoint', type=str, help='Endpoint of Upstream server')
    parser.add_argument('--working_dir', type=str, help='Current working directory')
    parser.add_argument('--zone_id', type=str, help='Zone ID')
    args = parser.parse_args()
    endpoint = args.endpoint
    working_dir = args.working_dir if args.working_dir != '.' else os.getcwd()
    zone_id = args.zone_id
    _run_server(endpoint, working_dir, zone_id)
