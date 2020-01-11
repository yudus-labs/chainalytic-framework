import json
import time
from typing import Dict, List, Optional, Set, Tuple, Union

import plyvel
from iconservice.icon_config import default_icon_config
from iconservice.icon_constant import ConfigKey
from iconservice.iiss.engine import Engine

from chainalytic.aggregator.transform import BaseTransform
from chainalytic.common import rpc_client, trie


class Transform(BaseTransform):
    START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT = 7597365

    LAST_STATE_HEIGHT_KEY = b'last_state_height'

    def __init__(self, working_dir: str, zone_id: str, transform_id: str):
        super(Transform, self).__init__(working_dir, zone_id, transform_id)

    async def execute(self, height: int, input_data: dict) -> Optional[Dict]:
        start_time = time.time()

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

        set_stake_wallets = input_data['data']

        if set_stake_wallets:
            updated_stake_top100 = cache_db.get(b'stake_top100')
            if updated_stake_top100:
                updated_stake_top100 = json.loads(updated_stake_top100)
            else:
                updated_stake_top100 = {}

            for addr, val in set_stake_wallets.items():
                updated_stake_top100[addr] = val

            updated_stake_top100 = {
                k: v
                for k, v in sorted(
                    updated_stake_top100.items(), key=lambda item: item[1], reverse=1
                )
            }
            top100_addresses = list(updated_stake_top100)[:100]
            updated_stake_top100 = {
                k: v for k, v in updated_stake_top100.items() if k in top100_addresses
            }
            cache_db_batch.put(b'stake_top100', json.dumps(updated_stake_top100).encode())
        else:
            updated_stake_top100 = None

        cache_db_batch.put(Transform.LAST_STATE_HEIGHT_KEY, str(height).encode())
        cache_db_batch.write()

        # execution_time = f'{round(time.time()-start_time, 4)}s'

        return {
            'height': height,
            'data': {},
            'misc': {'latest_stake_top100': {'wallets': updated_stake_top100, 'height': height}},
        }
