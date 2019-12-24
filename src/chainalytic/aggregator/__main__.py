import os
import sys
import argparse
import websockets
import asyncio
from time import time
from jsonrpcserver import method
from jsonrpcclient.clients.websockets_client import WebSocketsClient
from chainalytic.common import config, rpc_client
from chainalytic.common.rpc_server import EXIT_SERVICE, main_dispatcher, show_call_info
from . import Aggregator

_AGGREGATOR = None


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
    elif call_id == 'exit':
        return EXIT_SERVICE
    else:
        return 'Not implemented'


async def init_state():
    print('Initializing Aggregator service...')
    warehouse_endpoint = _AGGREGATOR.warehouse_endpoint

    # Set last_block_height value for the first time
    for tid in _AGGREGATOR.kernel.transforms:
        warehouse_response = await rpc_client.call_async(
            warehouse_endpoint, call_id='last_block_height', transform_id=tid
        )
        if warehouse_response['status'] and not warehouse_response['data']:
            await rpc_client.call_async(
                warehouse_endpoint,
                call_id='set_last_block_height',
                height=_AGGREGATOR.kernel.START_BLOCK_HEIGHT,
                transform_id=tid,
            )
            print('Set initial last_block_height')
    print('Initialized Aggregator service')


async def fetch_data():
    upstream_endpoint = _AGGREGATOR.upstream_endpoint
    warehouse_endpoint = _AGGREGATOR.warehouse_endpoint

    while 1:
        print('New aggregation')
        t1 = time()
        for tid in _AGGREGATOR.kernel.transforms:
            print('--Trying to fetch data...')
            print(f'----For transform: {tid}')
            print(f'----From Upstream: {upstream_endpoint}')

            warehouse_response = await rpc_client.call_async(
                warehouse_endpoint, call_id='last_block_height', transform_id=tid
            )
            print(f'----Last block height: {warehouse_response["data"]}')

            if warehouse_response['status'] and type(warehouse_response['data']) == int:
                next_block_height = warehouse_response['data'] + 1
                upstream_response = await rpc_client.call_async(
                    upstream_endpoint, call_id='get_block', height=next_block_height
                )
                if upstream_response['status'] and upstream_response['data'] is not None:
                    print('--Fetched data successfully')
                    print(f'--Next block height: {next_block_height}')
                    print('--Preparing to execute next block...')
                    await _AGGREGATOR.kernel.execute(
                        height=next_block_height,
                        input_data=upstream_response['data'],
                        transform_id=tid,
                    )
                    print(f'--Executed block "{next_block_height}" successfully')
            print('--')
        agg_time = round(time() - t1, 4)
        print(f'Total aggregation time: {agg_time}s')
        print(f'Estimated aggregation speed: {int(1/agg_time)} blocks/s')
        print('')


def _run_server(endpoint, working_dir, zone_id):
    global _AGGREGATOR
    _AGGREGATOR = Aggregator(working_dir, zone_id)
    print(f'Aggregator endpoint: {endpoint}')
    print(f'Aggregator zone ID: {zone_id}')

    host = endpoint.split(':')[0]
    port = int(endpoint.split(':')[1])

    asyncio.get_event_loop().run_until_complete(init_state())
    asyncio.get_event_loop().create_task(fetch_data())

    start_server = websockets.serve(main_dispatcher, host, port)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()
    print('Exited Aggregator')


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
