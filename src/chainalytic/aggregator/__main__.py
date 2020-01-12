import argparse
import asyncio
import os
import sys
from time import time

import websockets
from jsonrpcclient.clients.websockets_client import WebSocketsClient
from jsonrpcserver import method

from chainalytic.common import config, rpc_client, rpc_server
from chainalytic.common.rpc_server import EXIT_SERVICE, main_dispatcher, show_call_info
from chainalytic.common.util import create_logger

from . import Aggregator

_AGGREGATOR = None

_LOGGER = None


@method
async def _call(call_id: str, **kwargs):
    params = kwargs
    show_call_info(call_id, params)

    if call_id == 'ping':
        message = '\n'.join(
            [
                'Pong !',
                'Aggregator service is running',
                f'Working dir: {_AGGREGATOR.working_dir}',
                f'Params: {params}',
            ]
        )
        return message
    elif call_id == 'get_zone_id':
        return _AGGREGATOR.zone_id
    elif call_id == 'exit':
        return EXIT_SERVICE
    elif call_id == 'ls_all_transform_id':
        return list(_AGGREGATOR.kernel.transforms)
    else:
        return 'Not implemented'


async def initialize():
    _LOGGER.info('Initializing Aggregator service...')
    warehouse_endpoint = _AGGREGATOR.warehouse_endpoint

    _LOGGER.info('Waiting for Warehouse service...')
    warehouse_response = await rpc_client.call_async(warehouse_endpoint, call_id='ping')
    while not warehouse_response['status']:
        warehouse_response = await rpc_client.call_async(warehouse_endpoint, call_id='ping')

    # Set last_block_height value for the first time
    for tid in _AGGREGATOR.kernel.transforms:
        warehouse_response = await rpc_client.call_async(
            warehouse_endpoint,
            call_id='api_call',
            api_id='last_block_height',
            api_params={'transform_id': tid},
        )
        if not warehouse_response['data']:
            await rpc_client.call_async(
                warehouse_endpoint,
                call_id='api_call',
                api_id='set_last_block_height',
                api_params={
                    'height': _AGGREGATOR.kernel.transforms[tid].START_BLOCK_HEIGHT - 1,
                    'transform_id': tid,
                },
            )
            _LOGGER.info(f'--Set initial last_block_height for transform: {tid}')
    _LOGGER.info('Initialized Aggregator service')
    _LOGGER.info()


async def fetch_data():
    upstream_endpoint = _AGGREGATOR.upstream_endpoint
    warehouse_endpoint = _AGGREGATOR.warehouse_endpoint

    while 1:
        _LOGGER.info('New aggregation')

        t = time()
        for tid in _AGGREGATOR.kernel.transforms:
            t1 = time()
            _LOGGER.info(f'|Transform ID: {tid}')
            _LOGGER.info(f'|--Trying to fetch data...')
            _LOGGER.info(f'|----From Upstream: {upstream_endpoint}')

            warehouse_response = await rpc_client.call_async(
                warehouse_endpoint,
                call_id='api_call',
                api_id='last_block_height',
                api_params={'transform_id': tid},
            )
            _LOGGER.info(f'|----Last block height: {warehouse_response["data"]}')

            if warehouse_response['status'] and type(warehouse_response['data']) == int:
                next_block_height = warehouse_response['data'] + 1
                upstream_response = await rpc_client.call_async(
                    upstream_endpoint,
                    call_id='get_block',
                    height=next_block_height,
                    transform_id=tid,
                )
                if upstream_response['status'] and upstream_response['data'] is not None:
                    _LOGGER.info(f'|--Fetched data successfully')
                    _LOGGER.info(f'|--Next block height: {next_block_height}')
                    _LOGGER.info(f'|--Preparing to execute next block...')
                    await _AGGREGATOR.kernel.execute(
                        height=next_block_height,
                        input_data=upstream_response['data'],
                        transform_id=tid,
                    )
                    _LOGGER.info(f'|--Executed block "{next_block_height}" successfully')
                else:
                    _LOGGER.info(f'|--Failed to fetch, trying again...')
            agg_time = round(time() - t1, 4)
            _LOGGER.info(f'|Aggregation time: {agg_time}s')

        _LOGGER.info('|')
        tagg_time = round(time() - t, 4)
        _LOGGER.info(f'|Total aggregation time: {tagg_time}s')
        _LOGGER.info(f'|Estimated aggregation speed: {int(1/tagg_time)} blocks/s')
        _LOGGER.info('')
        _LOGGER.info('')


def _run_server(endpoint, working_dir, zone_id):
    global _AGGREGATOR
    global _LOGGER
    _LOGGER = create_logger('aggregator', zone_id)
    rpc_server.set_logger(_LOGGER)

    _AGGREGATOR = Aggregator(working_dir, zone_id)
    _LOGGER.info(f'Aggregator endpoint: {endpoint}')
    _LOGGER.info(f'Aggregator zone ID: {zone_id}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])

    asyncio.get_event_loop().run_until_complete(initialize())
    asyncio.get_event_loop().create_task(fetch_data())

    start_server = websockets.serve(main_dispatcher, host, port)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
    _LOGGER.info('Exited Aggregator')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Chainalytic Aggregator server')
    parser.add_argument('--endpoint', type=str, help='Endpoint of Aggregator server')
    parser.add_argument('--working_dir', type=str, help='Current working directory')
    parser.add_argument('--zone_id', type=str, help='Zone ID')
    args = parser.parse_args()
    endpoint = args.endpoint
    working_dir = args.working_dir if args.working_dir != '.' else os.getcwd()
    zone_id = args.zone_id
    _run_server(endpoint, working_dir, zone_id)
