"""
Sample service calls

from chainalytic.common import rpc_client
rpc_client.call(f'localhost:5520', call_id='put_block', height=999, data={'cool': 'day'}, transform_id='unstaking')
rpc_client.call(f'localhost:5520', call_id='get_block', height=999, transform_id='unstaking')
rpc_client.call(f'localhost:5520', call_id='last_block_height', transform_id='unstaking')

"""

import os
import argparse
import websockets
import asyncio
import time
from jsonrpcserver import method
from chainalytic.common.rpc_server import EXIT_SERVICE, main_dispatcher
from . import Warehouse

_WAREHOUSE = None


@method
async def _call(call_id: str, **kwargs):
    params = kwargs
    print(f'Call: {call_id}')
    if call_id == 'ping':
        message = ''.join(
            [
                'Pong !\n',
                'Warehouse service is running\n',
                f'Working dir: {_WAREHOUSE.working_dir}\n',
                f'Params: {params}',
            ]
        )
        return message
    elif call_id == 'exit':
        return EXIT_SERVICE
    elif call_id == 'put_block':
        height = params['height']
        data = params['data']
        transform_id = params['transform_id']
        return await _WAREHOUSE.storage.put_block(height, data, transform_id)
    elif call_id == 'get_block':
        height = params['height']
        transform_id = params['transform_id']
        return await _WAREHOUSE.storage.get_block(height, transform_id)
    elif call_id == 'last_block_height':
        transform_id = params['transform_id']
        return await _WAREHOUSE.storage.last_block_height(transform_id)
    else:
        return f'Not implemented'


def _run_server(endpoint, working_dir, zone_id):
    global _WAREHOUSE
    _WAREHOUSE = Warehouse(working_dir, zone_id)
    print(f'Warehouse endpoint: {endpoint}')
    print(f'Warehouse zone ID: {zone_id}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])
    start_server = websockets.serve(main_dispatcher, host, port)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
    print('Initialized Warehouse')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Chainalytic Warehouse server')
    parser.add_argument('--endpoint', type=str, help='Endpoint of Warehouse server')
    parser.add_argument('--working_dir', type=str, help='Current working directory')
    parser.add_argument('--zone_id', type=str, help='Zone ID')
    args = parser.parse_args()
    endpoint = args.endpoint
    working_dir = args.working_dir if args.working_dir != '.' else os.getcwd()
    zone_id = args.zone_id
    _run_server(endpoint, working_dir, zone_id)
