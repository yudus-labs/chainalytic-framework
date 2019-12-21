from typing import List, Set, Dict, Tuple, Optional
import asyncio
import websockets
import argparse
import traceback
from jsonrpcclient.clients.websockets_client import WebSocketsClient

SUCCEED_STATUS = 1
FAILED_STATUS = 0


async def _main(endpoint: str, **kwargs) -> Dict:
    try:
        async with websockets.connect(f"ws://{endpoint}") as ws:
            r = await WebSocketsClient(ws).request("_call", **kwargs)
        return {'status': SUCCEED_STATUS, 'data': r.data.result}
    except Exception as e:
        return {'status': FAILED_STATUS, 'data': f'{str(e)}\n{traceback.format_exc()}'}


def call(endpoint: str, **kwargs) -> Dict:
    """Use this function to communicate with all Chainalytic services

    Default service endpoints:
        Aggregator: localhost:5500
        Warehouse: localhost:5510
        Provider: localhost:5520
    
    Returns:
        dict: {'status': bool, 'data': Any}
    """
    return asyncio.get_event_loop().run_until_complete(_main(endpoint, **kwargs))
