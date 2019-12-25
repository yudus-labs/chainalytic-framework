import sys
from typing import Dict, List, Optional, Set, Tuple

from jsonrpcserver import async_dispatch as dispatch

from chainalytic.common import util

EXIT_SERVICE = '__EXIT_SERVICE__'


async def main_dispatcher(websocket, path):
    response = await dispatch(await websocket.recv())
    if response.wanted:
        await websocket.send(str(response))
        if hasattr(response, 'result'):
            if response.result == EXIT_SERVICE:
                print('Service is terminated')
                sys.exit()


def show_call_info(call_id: str, params: dict):
    print(f'Call: {call_id}')
    print('Params:')
    print(util.pretty(params))
    print()
