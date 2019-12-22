from typing import List, Set, Dict, Tuple, Optional, Union
import json
import plyvel
from chainalytic.common import rpc_client, trie
from chainalytic.aggregator.transform import BaseTransform

from iconservice.iiss.engine import Engine
from iconservice.icon_constant import ConfigKey
from iconservice.icon_config import default_icon_config


def unlock_period(total_stake):
    total_supply = 805000000

    p = Engine._calculate_unstake_lock_period(
        default_icon_config[ConfigKey.IISS_META_DATA][ConfigKey.UN_STAKE_LOCK_MIN],
        default_icon_config[ConfigKey.IISS_META_DATA][ConfigKey.UN_STAKE_LOCK_MAX],
        default_icon_config[ConfigKey.IISS_META_DATA][ConfigKey.REWARD_POINT],
        total_stake,
        total_supply,
    )

    return p


class Transform(BaseTransform):
    PREV_STATE_KEY = b'prev_state'
    PREV_STATE_HEIGHT_KEY = b'prev_state_height'

    def __init__(self, working_dir: str, zone_id: str, transform_id: str):
        super(Transform, self).__init__(working_dir, zone_id, transform_id)

    async def execute(self, height: int, input_data: Dict) -> Optional[Dict]:
        # Load transform cache to retrive previous state trie
        #
        cache_db = plyvel.DB(self.transform_cache_dir, create_if_missing=True)
        prev_state = cache_db.get(Transform.PREV_STATE_KEY)
        if prev_state:
            prev_trie = trie.Trie()
            prev_trie.from_hex(prev_state.decode())
        else:
            prev_trie = None

        # Make sure input block data represents for the next block of previous state cache
        #
        prev_state_height = cache_db.get(Transform.PREV_STATE_HEIGHT_KEY)
        if prev_state_height:
            prev_state_height = int(prev_state_height)
            if prev_state_height != height - 1:
                await rpc_client.call_async(
                    self.warehouse_endpoint,
                    call_id='set_last_block_height',
                    height=prev_state_height,
                    transform_id=self.transform_id,
                )
                return None

        # Retrieve previous block data from Storage service
        #
        prev_block = await rpc_client.call_async(
            self.warehouse_endpoint,
            call_id='get_block',
            height=height - 1,
            transform_id=self.transform_id,
        )
        try:
            prev_block = json.loads(prev_block['data'])
        except Exception:
            prev_block = None

        if prev_block:
            prev_total_staking = prev_block['total_staking']
        else:
            prev_total_staking = 0

        # Process current block
        #
        if height < self.kernel.V3_BLOCK_HEIGHT:
            txs = input_data['confirmed_transaction_list']
        else:
            txs = input_data['transactions']

        set_stake_wallets = {}
        for tx in txs:
            if 'data' not in tx:
                continue
            if 'method' not in tx['data']:
                continue
            if tx['data']['method'] == 'setStake':
                stake_value = int(tx['data']['params']['value'], 16) / 10 ** 18
                set_stake_wallets[tx["from"]] = stake_value

        # Calculate unstaking for each wallet and put it to transform cache
        if prev_trie:
            for addr in set_stake_wallets:
                prev_stake_value = prev_trie.get_value(addr)
                cur_stake_value = set_stake_wallets[addr]
                prev_unstaking_value = cache_db.get(addr.encode())
                cur_unstaking_value = None

                if prev_stake_value:
                    prev_stake_value = float(prev_stake_value)
                    # Unstake
                    if cur_stake_value < prev_stake_value:
                        if prev_unstaking_value:
                            prev_unstaking_value = int(prev_unstaking_value)
                            cur_unstaking_value = prev_unstaking_value + \
                                (prev_stake_value - cur_stake_value)
                        else:
                            cur_unstaking_value = 0

                    # Restake
                    else:
                        if prev_unstaking_value:
                            prev_unstaking_value = int(prev_unstaking_value)
                            cur_unstaking_value = prev_unstaking_value - \
                                (cur_stake_value - prev_stake_value)
                        else:
                            cur_unstaking_value = 0

                    cur_unstaking_value = 0 if cur_unstaking_value < 0 else cur_unstaking_value

                    cache_db.put(addr.encode(), str(cur_unstaking_value).encode())
                    # TOOD: take unlock-period into accound

        # Calculate total unstaking
        total_unstaking = 0
        for k, v in cache_db:
            if k.startswith(b'hx'):
                total_unstaking += float(v)

        # Update current state trie
        cur_trie = prev_trie if prev_trie else trie.Trie()

        for addr in set_stake_wallets:
            # It overrides existing addresses
            cur_trie.add_path(f'{addr}:{set_stake_wallets[addr]}')

            # TODO: remove address with zero stake from trie
            # TODO: clean expired unlock period

        staking_values = [float(v) for v in cur_trie.ls_values() if float(v) > 0]
        total_staking = sum(staking_values)

        # Save latest state to transform cache
        cur_state = cur_trie.to_hex().encode()
        cache_db.put(Transform.PREV_STATE_KEY, cur_state)
        cache_db.put(Transform.PREV_STATE_HEIGHT_KEY, str(height).encode())

        data = {
            'total_staking': total_staking,
            'total_unstaking': total_unstaking,
            'total_staking_wallets': len(staking_values)
        }

        return {'height': height, 'data': data}
