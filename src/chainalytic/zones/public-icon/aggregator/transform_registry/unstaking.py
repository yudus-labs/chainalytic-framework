from typing import List, Set, Dict, Tuple, Optional, Union
import json
from chainalytic.common import rpc_client, trie
from chainalytic.aggregator.transform import BaseTransform


class Transform(BaseTransform):

    def __init__(self, working_dir: str, zone_id: str, transform_id: str):
        super(Transform, self).__init__(working_dir, zone_id, transform_id)

    async def execute(self, height: int, input_data: Dict) -> Dict:
        # Retrieve previous block
        #
        prev_block = await rpc_client.call_async(
            self.warehouse_endpoint,
            call_id='get_block',
            height=height - 1,
            transform_id=self.transform_id
        )
        try:
            prev_block = json.loads(prev_block['data'])
        except Exception:
            prev_block = None

        if prev_block:
            prev_staking = prev_block['staking']
            prev_trie = trie.Trie()
            prev_trie.from_hex(prev_block['state_trie'])
        else:
            prev_staking = 0
            prev_trie = None

        # Process current block
        #
        if height < self.kernel.V3_BLOCK_HEIGHT:
            txs = input_data['confirmed_transaction_list']
        else:
            txs = input_data['transactions']

        staked_wallets = []
        for tx in txs:
            if 'data' not in tx:
                continue
            if 'method' not in tx['data']:
                continue
            if tx['data']['method'] == 'setStake':
                stake_value = int(tx['data']['params']['value'], 16) / 10**18
                staked_wallets.append(
                    f'{tx["from"]}:{stake_value}'
                )

        cur_trie = prev_trie if prev_trie else trie.Trie()

        for addr in staked_wallets:
            cur_trie.add_path(addr)

        staking = sum([float(v) for v in cur_trie.ls_values()])
        unstaking = prev_staking - staking if staking < prev_staking else 0

        state = cur_trie.to_hex()

        data = {'state_trie': state, 'staking': staking, 'unstaking': unstaking}

        return {'height': height, 'data': data}
