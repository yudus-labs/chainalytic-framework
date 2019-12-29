"""
Exposed APIs

from chainalytic.common import rpc_client

rpc_client.call_async(
    'localhost:5520',
    call_id='api_call',
    api_id='put_block',
    api_params={
        height: int,
        data: Union,
        transform_id: str,
    }
)
rpc_client.call_async(
    'localhost:5520',
    call_id='api_call',
    api_id='get_block',
    api_params={
        height: int,
        transform_id: str,
    }
)
rpc_client.call_async(
    'localhost:5520',
    call_id='api_call',
    api_id='last_block_height',
    api_params={
        transform_id: str,
    }
)
rpc_client.call_async(
    'localhost:5520',
    call_id='api_call',
    api_id='set_last_block_height',
    api_params={
        height: int,
        transform_id: str,
    }
)
rpc_client.call_async(
    'localhost:5520',
    call_id='api_call',
    api_id='set_latest_unstake_state',
    api_params={
        unstake_state: dict,
        transform_id: str,
    }
)
rpc_client.call_async(
    'localhost:5520',
    call_id='api_call',
    api_id='latest_unstake_state',
    api_params={
        transform_id: str,
    }
)

"""

import json
from pathlib import Path
from typing import Collection, Dict, List, Optional, Set, Tuple, Union

import plyvel

from chainalytic.common import config, zone_manager
from chainalytic.warehouse.storage import BaseStorage


class Storage(BaseStorage):
    LATEST_UNSTAKE_STATE_KEY = b'latest_unstake_state'

    def __init__(self, working_dir: str, zone_id: str):
        super(Storage, self).__init__(working_dir, zone_id)

    async def put_block(self, api_params: dict) -> bool:
        """Put block data to one specific transform storage.

        `last_block_height` value is also updated here
        """

        height: int = api_params['height']
        data: Union[Collection, bytes, str, float, int] = api_params['data']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        key = str(height).encode()

        if isinstance(data, dict):
            value = json.dumps(data).encode()
        elif isinstance(data, (list, tuple)):
            value = str(data).encode()
        elif isinstance(data, str):
            value = data.encode()
        elif isinstance(data, (int, float)):
            value = str(data).encode()
        elif isinstance(data, bytes):
            value = data
        else:
            return 0

        db.put(key, value)
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, key)

        return 1

    async def get_block(self, api_params: dict) -> Optional[str]:
        """Get block data from one specific transform storage."""

        height: int = api_params['height']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        key = str(height).encode()
        value = db.get(key)
        value = value.decode() if value else value

        return value

    async def last_block_height(self, api_params: dict) -> Optional[int]:
        """Get last block height in one specific transform storage."""

        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        value = db.get(Storage.LAST_BLOCK_HEIGHT_KEY)

        try:
            height = int(value)
        except Exception:
            height = None

        return height

    async def set_last_block_height(self, api_params: dict) -> bool:
        """Set last block height in one specific transform storage."""

        height: int = api_params['height']
        transform_id: str = api_params['transform_id']

        try:
            height = int(height)
            value = str(height).encode()
        except Exception:
            return 0

        db = self.transform_storage_dbs[transform_id]
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, value)

        return 1

    async def set_latest_unstake_state(self, api_params: dict) -> bool:

        unstake_state: dict = api_params['unstake_state']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        db.put(Storage.LATEST_UNSTAKE_STATE_KEY, value=json.dumps(unstake_state).encode())

        return 1

    async def latest_unstake_state(self, api_params: dict) -> Optional[str]:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        value = db.get(Storage.LATEST_UNSTAKE_STATE_KEY)
        value = value.decode() if value else value

        return value
