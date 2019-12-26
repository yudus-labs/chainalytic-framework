"""
Sample service calls

from chainalytic.common import rpc_client

rpc_client.call_aiohttp(
    f'localhost:5530',
    call_id='api_call',
    api_id='get_staking_info',
    api_params={'height': 999999}
)
rpc_client.call_aiohttp(
    f'localhost:5530',
    call_id='api_call',
    api_id='last_block_height',
    api_params={'transform_id': 'stake_history'}
)

"""

import argparse
import asyncio
import os
import sys
import time

import websockets
from aiohttp import web
from jsonrpcserver import async_dispatch as dispatch
from jsonrpcserver import method

from chainalytic.common.rpc_server import (EXIT_SERVICE, main_dispatcher,
                                           show_call_info)

from . import Provider

_PROVIDER = None


@method
async def _call(call_id: str, **kwargs):
    params = kwargs
    show_call_info(call_id, params)

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
        print('Service is terminated')
        sys.exit()
        return EXIT_SERVICE
    elif call_id == 'api_call':
        api_id = params['api_id']
        api_params = params['api_params']
        return await _PROVIDER.api_bundle.api_call(api_id, api_params)
    else:
        return f'Not implemented'


# websockets transfer protocol
# def _run_server(endpoint, working_dir, zone_id):
#     global _PROVIDER
#     _PROVIDER = Provider(working_dir, zone_id)
#     print(f'Provider endpoint: {endpoint}')
#     print(f'Provider zone ID: {zone_id}')

#     host = endpoint.split(':')[0]
#     port = int(endpoint.split(':')[1])
#     start_server = websockets.serve(main_dispatcher, host, port)
#     asyncio.get_event_loop().run_until_complete(start_server)
#     asyncio.get_event_loop().run_forever()
#     print('Initialized Provider')

# aiohttp transfer protocol
def _run_server(endpoint, working_dir, zone_id):
    global _PROVIDER
    _PROVIDER = Provider(working_dir, zone_id)
    print(f'Provider endpoint: {endpoint}')
    print(f'Provider zone ID: {zone_id}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])

    async def handle(request):
        request = await request.text()
        response = await dispatch(request)
        if response.wanted:
            return web.json_response(response.deserialized(), status=response.http_status)
        else:
            return web.Response()

    app = web.Application()
    app.router.add_post("/", handle)
    web.run_app(app, port=port)

    print('Exited Provider')


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
