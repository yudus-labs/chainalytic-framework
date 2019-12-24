from typing import List, Set, Dict, Tuple, Optional
import json
from time import time
from pprint import pprint
from pathlib import Path
from chainalytic.common import config, zone_manager
from chainalytic.upstream.data_feeder import BaseDataFeeder
import plyvel

BLOCK_HEIGHT_KEY = b'block_height_key'
BLOCK_HEIGHT_BYTES_LEN = 12

FIRST_STAKE_BLOCK_HEIGHT = 7597365
START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT - 1
V3_BLOCK_HEIGHT = 10324749


class DataFeeder(BaseDataFeeder):
    def __init__(self, working_dir: str, zone_id: str):
        super(DataFeeder, self).__init__(working_dir, zone_id)

    async def get_block(self, height: int, verbose: bool = 0) -> Optional[dict]:
        """Retrieve standard block data from chain
        """
        if verbose:
            print(f'Feeding block: {height}')

        t1 = time()
        db = self.chain_db
        if verbose:
            print(f'--Time to init leveldb: {round(time() - t1, 4)}s')

        t11 = time()
        block_hash = db.get(
            BLOCK_HEIGHT_KEY + height.to_bytes(BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
        )
        if verbose:
            print(f'--Time to find block hash: {round(time() - t11, 4)}s')

        if not block_hash:
            return None

        try:
            t2 = time()
            data = db.get(block_hash)
            if verbose:
                print(f'--Time to load block from leveldb: {round(time() - t2, 4)}s')

            t3 = time()
            block = json.loads(data)
            if verbose:
                print(f'--Time to convert data: {round(time() - t3, 4)}s')

            if height < V3_BLOCK_HEIGHT:
                txs = block['confirmed_transaction_list']
            else:
                txs = block['transactions']
        except Exception:
            return None

        if verbose:
            print(f'Total time to load data: {round(time() - t1, 4)}s')

        t4 = time()
        set_stake_wallets = {}
        for tx in txs:
            if 'data' not in tx:
                continue
            if 'method' not in tx['data']:
                continue
            if tx['data']['method'] == 'setStake':
                stake_value = int(tx['data']['params']['value'], 16) / 10 ** 18
                set_stake_wallets[tx["from"]] = stake_value
        if verbose:
            print(f'Time to pre-process data: {round(time() - t4, 4)}s')

            print(f'Total feeding time: {round(time() - t1, 4)}s')
            print()

        return set_stake_wallets
