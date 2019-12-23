from typing import List, Set, Dict, Tuple, Optional, Union
import json
import plyvel
from chainalytic.common import rpc_client, trie
from chainalytic.aggregator.transform import BaseTransform

from iconservice.iiss.engine import Engine
from iconservice.icon_constant import ConfigKey
from iconservice.icon_config import default_icon_config


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
    PREV_STATE_HEIGHT_KEY = b'prev_state_height'

    def __init__(self, working_dir: str, zone_id: str, transform_id: str):
        super(Transform, self).__init__(working_dir, zone_id, transform_id)

    def _get_total_supply(self) -> int:
        db = plyvel.DB(self.kernel.icon_state_db_dir)
        r = db.get(b'total_supply')
        return int.from_bytes(r, 'big')/10**18

    async def execute(self, height: int, input_data: list) -> Optional[Dict]:
        # Load transform cache to retrive previous staking state
        #
        cache_db = plyvel.DB(self.transform_cache_dir, create_if_missing=True)

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

        # #################################################
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
            prev_total_unstaking = prev_block['total_unstaking']
            prev_total_staking_wallets = prev_block['total_staking_wallets']
        else:
            prev_total_staking = 0
            prev_total_unstaking = 0
            prev_total_staking_wallets = 0

        total_staking = prev_total_staking
        total_unstaking = prev_total_unstaking
        total_staking_wallets = prev_total_staking_wallets

        # Find wallets that set new stake in current block
        #
        txs = input_data
        set_stake_wallets = {}
        for tx in txs:
            if 'data' not in tx:
                continue
            if 'method' not in tx['data']:
                continue
            if tx['data']['method'] == 'setStake':
                stake_value = int(tx['data']['params']['value'], 16) / 10 ** 18
                set_stake_wallets[tx["from"]] = stake_value

        # Cleanup expired unlock period
        #
        unstaking_addresses = cache_db.get(b'unstaking')
        if unstaking_addresses:
            unstaking_addresses = json.loads(unstaking_addresses)
        else:
            unstaking_addresses = {}
        for addr in unstaking_addresses.keys():
            if unstaking_addresses[addr] <= height:
                unstaking_addresses.pop(addr)

                stake_value, unstaking_value, unlock_height = addr_data.split(b':')
                stake_value = float(stake_value)
                cache_db.put(addr.encode(), f'{stake_value}:0:0'.encode())

        cache_db.put(b'unstaking', json.dumps(unstaking_addresses).encode())

        # Calculate staking, unstaking and unlock_height for each wallet
        # and put them to transform cache
        #
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
                    cur_unstaking_value = prev_unstaking_value + \
                        (prev_stake_value - cur_stake_value)

                else:
                    cur_unstaking_value = prev_stake_value - cur_stake_value
                unlock_height = height + unlock_period(prev_total_staking, self._get_total_supply())

            # Restake
            else:
                if prev_unstaking_value > 0:
                    cur_unstaking_value = prev_unstaking_value - \
                        (cur_stake_value - prev_stake_value)
                else:
                    cur_unstaking_value = 0

            if cur_unstaking_value <= 0:
                cur_unstaking_value = 0
                unlock_height = 0

            cache_db.put(
                addr.encode(),
                f'{cur_stake_value}:{cur_unstaking_value}:{unlock_height}'.encode()
            )

            # Update unstaking wallets list
            if unlock_height:
                unstaking_addresses = cache_db.get(b'unstaking')
                if unstaking_addresses:
                    unstaking_addresses = json.loads(unstaking_addresses)
                else:
                    unstaking_addresses = {}
                unstaking_addresses[addr] = unlock_height
                cache_db.put(b'unstaking', json.dumps(unstaking_addresses).encode())

            # Update total staking and unstaking
            total_staking = total_staking - prev_stake_value + cur_stake_value
            total_unstaking = total_unstaking - prev_unstaking_value + cur_unstaking_value

        cache_db.put(Transform.PREV_STATE_HEIGHT_KEY, str(height).encode())

        data = {
            'total_staking': total_staking,
            'total_unstaking': total_unstaking,
            'total_staking_wallets': total_staking_wallets
        }

        return {'height': height, 'data': data}
