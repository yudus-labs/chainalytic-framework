"""
Exposed APIs

api_id='put_block'
api_params={
    height: int,
    data: Union,
    transform_id: str,
}

api_id='get_block'
api_params={
    height: int,
    transform_id: str,
}

api_id='last_block_height'
api_params={
    transform_id: str,
}

api_id='set_last_block_height'
api_params={
    height: int,
    transform_id: str,
}

api_id='set_latest_unstake_state'
api_params={
    unstake_state: dict,
    transform_id: 'stake_history',
}

api_id='latest_unstake_state'
api_params={
    transform_id: 'stake_history',
}

api_id='set_latest_stake_top100'
api_params={
    unstake_state: dict,
    transform_id: 'stake_top100',
}

api_id='latest_stake_top100'
api_params={
    transform_id: 'stake_top100',
}

api_id='set_recent_stake_wallets'
api_params={
    unstake_state: dict,
    transform_id: 'recent_stake_wallets',
}

api_id='recent_stake_wallets'
api_params={
    transform_id: 'recent_stake_wallets',
}

api_id='set_abstention_stake'
api_params={
    unstake_state: dict,
    transform_id: 'abstention_stake',
}

api_id='abstention_stake'
api_params={
    transform_id: 'abstention_stake',
}

"""

import json
from pathlib import Path
from typing import Collection, Dict, List, Optional, Set, Tuple, Union

import plyvel

from chainalytic.common import config, zone_manager
from chainalytic.warehouse.storage import BaseStorage


class Storage(BaseStorage):
    LATEST_UNSTAKE_STATE_KEY = b'latest_unstake_state'
    LATEST_UNSTAKE_STATE_HEIGHT_KEY = b'latest_unstake_state_height'
    LATEST_STAKE_TOP100_KEY = b'latest_stake_top100'
    LATEST_STAKE_TOP100_HEIGHT_KEY = b'latest_stake_top100_height'
    RECENT_STAKE_WALLETS_KEY = b'recent_stake_wallets'
    RECENT_STAKE_WALLETS_HEIGHT_KEY = b'recent_stake_wallets_height'
    ABSTENTION_STAKE_KEY = b'abstention_stake'
    ABSTENTION_STAKE_HEIGHT_KEY = b'abstention_stake_height'

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

    ####################################
    # For `stake_history` transform only
    #
    async def set_latest_unstake_state(self, api_params: dict) -> bool:
        unstake_state: dict = api_params['unstake_state']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if unstake_state['wallets'] is not None:
            db.put(Storage.LATEST_UNSTAKE_STATE_KEY, json.dumps(unstake_state['wallets']).encode())
        db.put(
            Storage.LATEST_UNSTAKE_STATE_HEIGHT_KEY, str(unstake_state['height']).encode(),
        )

        return 1

    async def latest_unstake_state(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.LATEST_UNSTAKE_STATE_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.LATEST_UNSTAKE_STATE_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

    ###################################
    # For `stake_top100` transform only
    #
    async def set_latest_stake_top100(self, api_params: dict) -> bool:
        stake_top100: dict = api_params['stake_top100']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if stake_top100['wallets'] is not None:
            db.put(Storage.LATEST_STAKE_TOP100_KEY, json.dumps(stake_top100['wallets']).encode())
        db.put(
            Storage.LATEST_STAKE_TOP100_HEIGHT_KEY, str(stake_top100['height']).encode(),
        )
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, str(stake_top100['height']).encode())

        return 1

    async def latest_stake_top100(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.LATEST_STAKE_TOP100_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.LATEST_STAKE_TOP100_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

    ###########################################
    # For `recent_stake_wallets` transform only
    #
    async def set_recent_stake_wallets(self, api_params: dict) -> bool:
        recent_stake_wallets: dict = api_params['recent_stake_wallets']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if recent_stake_wallets['wallets'] is not None:
            db.put(
                Storage.RECENT_STAKE_WALLETS_KEY,
                json.dumps(recent_stake_wallets['wallets']).encode(),
            )
        db.put(
            Storage.RECENT_STAKE_WALLETS_HEIGHT_KEY, str(recent_stake_wallets['height']).encode(),
        )
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, str(recent_stake_wallets['height']).encode())

        return 1

    async def recent_stake_wallets(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.RECENT_STAKE_WALLETS_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.RECENT_STAKE_WALLETS_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

    #######################################
    # For `abstention_stake` transform only
    #
    async def set_abstention_stake(self, api_params: dict) -> bool:
        abstention_stake: dict = api_params['abstention_stake']
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        if abstention_stake['wallets'] is not None:
            db.put(
                Storage.ABSTENTION_STAKE_KEY, json.dumps(abstention_stake['wallets']).encode(),
            )
        db.put(
            Storage.ABSTENTION_STAKE_HEIGHT_KEY, str(abstention_stake['height']).encode(),
        )
        db.put(Storage.LAST_BLOCK_HEIGHT_KEY, str(abstention_stake['height']).encode())

        return 1

    async def abstention_stake(self, api_params: dict) -> dict:
        transform_id: str = api_params['transform_id']

        db = self.transform_storage_dbs[transform_id]
        wallets = db.get(Storage.ABSTENTION_STAKE_KEY)
        wallets = json.loads(wallets.decode()) if wallets else None
        height = db.get(Storage.ABSTENTION_STAKE_HEIGHT_KEY)
        height = int(height.decode()) if height else None

        return {'wallets': wallets, 'height': height}

