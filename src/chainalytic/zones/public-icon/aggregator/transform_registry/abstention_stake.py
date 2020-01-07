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

        set_stake_wallets = input_data['data']['stake']
        set_delegation_wallets = input_data['data']['delegation']
        state_changed = 1 if set_stake_wallets or set_delegation_wallets else 0

        abstention_stake = cache_db.get(b'abstention_stake')
        if abstention_stake:
            abstention_stake = json.loads(abstention_stake)
        else:
            abstention_stake = {}
            state_changed = 1

        # Process setStake txs
        for addr, val in set_stake_wallets.items():
            addr_data = cache_db.get(addr.encode())
            if addr_data:
                stake, delegation = addr_data.split(b':')
                stake = val
                delegation = float(delegation)
            else:
                stake = delegation = 0
                stake = val

            addr_data = f'{stake}:{delegation}'

            if stake - delegation > 1:
                abstention_stake[addr] = addr_data
            elif addr in abstention_stake:
                abstention_stake.pop(addr)

            cache_db_batch.put(addr.encode(), addr_data.encode())

        # Process setDelegation txs
        for addr, targets in set_delegation_wallets.items():
            delegation_val = 0
            for t in targets:
                try:
                    delegation_val += float(t['value'], 16) / 10 ** 18
                except Exception:
                    pass

            addr_data = cache_db.get(addr.encode())
            if addr_data:
                stake, delegation = addr_data.split(b':')
                stake = float(stake)
                delegation = delegation_val
            else:
                stake = delegation = 0
                delegation = delegation_val

            addr_data = f'{stake}:{delegation}'

            if stake - delegation > 1:
                abstention_stake[addr] = addr_data
            elif addr in abstention_stake:
                abstention_stake.pop(addr)

            cache_db_batch.put(addr.encode(), addr_data.encode())

        cache_db_batch.put(b'abstention_stake', json.dumps(abstention_stake).encode())
        cache_db_batch.put(Transform.LAST_STATE_HEIGHT_KEY, str(height).encode())
        cache_db_batch.write()

        # execution_time = f'{round(time.time()-start_time, 4)}s'

        return {
            'height': height,
            'data': {},
            'misc': {
                'abstention_stake': {
                    'wallets': abstention_stake if state_changed else None,
                    'height': height,
                }
            },
        }
