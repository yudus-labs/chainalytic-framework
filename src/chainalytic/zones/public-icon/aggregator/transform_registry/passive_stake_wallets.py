import json
import time
from typing import Dict, List, Optional, Set, Tuple, Union
import traceback

import plyvel
from iconservice.icon_config import default_icon_config
from iconservice.icon_constant import ConfigKey
from iconservice.iiss.engine import Engine

from chainalytic.aggregator.transform import BaseTransform
from chainalytic.common import rpc_client, trie


class Transform(BaseTransform):
    START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT = 7597365

    LAST_STATE_HEIGHT_KEY = b'last_state_height'
    MAX_WALLETS = 200

    def __init__(self, working_dir: str, zone_id: str, transform_id: str):
        super(Transform, self).__init__(working_dir, zone_id, transform_id)

    async def execute(self, height: int, input_data: dict) -> Optional[Dict]:
        # Load transform cache to retrive previous staking state
        cache_db = self.transform_cache_db
        cache_db_batch = self.transform_cache_db.write_batch()

        # Make sure input block data represents for the next block of previous state cache
        prev_state_height = cache_db.get(Transform.LAST_STATE_HEIGHT_KEY)
        if prev_state_height:
            prev_state_height = int(prev_state_height)
            if prev_state_height != height - 1:
                await rpc_client.call_async(
                    self.warehouse_endpoint,
                    call_id='api_call',
                    api_id='set_last_block_height',
                    api_params={'height': prev_state_height, 'transform_id': self.transform_id},
                )
                return None

        # #################################################

        set_delegation_wallets = input_data['data']['delegation']

        # Example of `updated_wallets`
        # {
        #     "ADDRESS_1": "12000000", # Block height
        #     "ADDRESS_2": "13000000",
        # }
        updated_wallets = {}

        for addr in set_delegation_wallets:
            updated_wallets[addr] = str(height)

        for addr, h in updated_wallets.items():
            cache_db_batch.put(addr.encode(), h.encode())

        cache_db_batch.put(Transform.LAST_STATE_HEIGHT_KEY, str(height).encode())
        cache_db_batch.write()

        return {
            'height': height,
            'data': {},
            'misc': {'updated_wallets': {'wallets': updated_wallets, 'height': height}},
        }
