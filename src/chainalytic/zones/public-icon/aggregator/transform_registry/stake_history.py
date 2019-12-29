import json
import time
from typing import Dict, List, Optional, Set, Tuple, Union

import plyvel
from iconservice.icon_config import default_icon_config
from iconservice.icon_constant import ConfigKey
from iconservice.iiss.engine import Engine

from chainalytic.aggregator.transform import BaseTransform
from chainalytic.common import rpc_client, trie


def unlock_period(total_stake, total_supply):
    p = Engine._calculate_unstake_lock_period(
        default_icon_config[ConfigKey.IISS_META_DATA][ConfigKey.UN_STAKE_LOCK_MIN],
        default_icon_config[ConfigKey.IISS_META_DATA][ConfigKey.UN_STAKE_LOCK_MAX],
        default_icon_config[ConfigKey.IISS_META_DATA][ConfigKey.REWARD_POINT],
        total_stake,
        total_supply,
    )

    return p


class Transform(BaseTransform):
    LAST_STATE_HEIGHT_KEY = b'last_state_height'
    LAST_TOTAL_STAKING_KEY = b'last_total_staking'
    LAST_TOTAL_UNSTAKING_KEY = b'last_total_unstaking'
    LAST_TOTAL_STAKING_WALLETS_KEY = b'last_total_staking_wallets'
    LAST_TOTAL_UNSTAKING_WALLETS_KEY = b'last_total_unstaking_wallets'

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

        # Read data of previous block from cache
        #
        prev_total_staking = cache_db.get(Transform.LAST_TOTAL_STAKING_KEY)
        prev_total_unstaking = cache_db.get(Transform.LAST_TOTAL_UNSTAKING_KEY)
        prev_total_staking_wallets = cache_db.get(Transform.LAST_TOTAL_STAKING_WALLETS_KEY)
        prev_total_unstaking_wallets = cache_db.get(Transform.LAST_TOTAL_UNSTAKING_WALLETS_KEY)

        prev_total_staking = float(prev_total_staking) if prev_total_staking else 0
        prev_total_unstaking = float(prev_total_unstaking) if prev_total_unstaking else 0
        prev_total_staking_wallets = (
            int(float(prev_total_staking_wallets)) if prev_total_staking_wallets else 0
        )
        prev_total_unstaking_wallets = (
            int(float(prev_total_unstaking_wallets)) if prev_total_unstaking_wallets else 0
        )

        total_staking = prev_total_staking
        total_unstaking = prev_total_unstaking
        total_staking_wallets = prev_total_staking_wallets
        total_unstaking_wallets = prev_total_unstaking_wallets

        # Cleanup expired unlock period
        #
        unstaking_addresses = cache_db.get(b'unstaking')
        if unstaking_addresses:
            unstaking_addresses = json.loads(unstaking_addresses)
        else:
            unstaking_addresses = {}
        for addr in list(unstaking_addresses):
            stake_value, unstaking_value, unlock_height = unstaking_addresses[addr].split(':')
            if int(unlock_height) <= height:
                unstaking_addresses.pop(addr)
                cache_db_batch.put(addr.encode(), f'{stake_value}:0:0'.encode())

        cache_db_batch.put(b'unstaking', json.dumps(unstaking_addresses).encode())

        # Calculate staking, unstaking and unlock_height for each wallet
        # and put them to transform cache
        # Only process wallets that set new stake in current block
        #
        set_stake_wallets = input_data['data']
        timestamp = input_data['timestamp']
        total_supply = input_data['total_supply']

        for addr in set_stake_wallets:
            addr_data = cache_db.get(addr.encode())

            if addr_data:
                prev_stake_value, prev_unstaking_value, unlock_height = addr_data.split(b':')
                prev_stake_value = float(prev_stake_value)
                prev_unstaking_value = float(prev_unstaking_value)
                unlock_height = int(unlock_height)
            else:
                prev_stake_value = prev_unstaking_value = unlock_height = 0

            cur_stake_value = set_stake_wallets[addr]
            cur_unstaking_value = 0

            if prev_stake_value == 0 and cur_stake_value > 0:
                total_staking_wallets += 1
            elif prev_stake_value > 0 and cur_stake_value == 0:
                total_staking_wallets -= 1

            # Unstake
            if cur_stake_value < prev_stake_value:
                if prev_unstaking_value > 0:
                    cur_unstaking_value = prev_unstaking_value + (
                        prev_stake_value - cur_stake_value
                    )

                else:
                    cur_unstaking_value = prev_stake_value - cur_stake_value
                unlock_height = height + unlock_period(prev_total_staking, total_supply)

            # Restake
            else:
                if prev_unstaking_value > 0:
                    cur_unstaking_value = prev_unstaking_value - (
                        cur_stake_value - prev_stake_value
                    )
                else:
                    cur_unstaking_value = 0

            if cur_unstaking_value <= 0:
                cur_unstaking_value = 0
                unlock_height = 0

            cache_db_batch.put(
                addr.encode(), f'{cur_stake_value}:{cur_unstaking_value}:{unlock_height}'.encode()
            )

            # Update unstaking wallets list
            if unlock_height:
                unstaking_addresses = cache_db.get(b'unstaking')
                if unstaking_addresses:
                    unstaking_addresses = json.loads(unstaking_addresses)
                else:
                    unstaking_addresses = {}
                unstaking_addresses[
                    addr
                ] = f'{cur_stake_value}:{cur_unstaking_value}:{unlock_height}'
                cache_db_batch.put(b'unstaking', json.dumps(unstaking_addresses).encode())

            # Update total staking and unstaking
            total_staking = total_staking - prev_stake_value + cur_stake_value
        total_unstaking_wallets = len(unstaking_addresses)

        cache_db_batch.put(Transform.LAST_STATE_HEIGHT_KEY, str(height).encode())
        cache_db_batch.put(Transform.LAST_TOTAL_STAKING_KEY, str(total_staking).encode())
        cache_db_batch.put(Transform.LAST_TOTAL_UNSTAKING_KEY, str(total_unstaking).encode())
        cache_db_batch.put(
            Transform.LAST_TOTAL_STAKING_WALLETS_KEY, str(total_staking_wallets).encode()
        )
        cache_db_batch.put(
            Transform.LAST_TOTAL_UNSTAKING_WALLETS_KEY, str(total_unstaking_wallets).encode()
        )
        cache_db_batch.write()

        # Calculate latest total unstaking and unstake state
        total_unstaking = 0
        for addr in unstaking_addresses:
            stake_value, unstaking_value, unlock_height = unstaking_addresses[addr].split(':')
            total_unstaking += float(unstaking_value)

        execution_time = f'{round(time.time()-start_time, 4)}s'

        data = {
            'total_staking': total_staking,
            'total_unstaking': total_unstaking,
            'total_staking_wallets': total_staking_wallets,
            'total_unstaking_wallets': total_unstaking_wallets,
            'execution_time': execution_time,
            'timestamp': timestamp,
        }

        return {
            'height': height,
            'data': data,
            'misc': {'latest_unstake_state': {'wallets': unstaking_addresses, 'height': height}},
        }
