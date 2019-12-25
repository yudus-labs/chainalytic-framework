import json
import traceback
from pathlib import Path
from pprint import pprint
from time import time
from typing import Dict, List, Optional, Set, Tuple

import plyvel

from chainalytic.common import config, zone_manager
from chainalytic.upstream.data_feeder import BaseDataFeeder

BLOCK_HEIGHT_KEY = b'block_height_key'
BLOCK_HEIGHT_BYTES_LEN = 12

FIRST_STAKE_BLOCK_HEIGHT = 7597365
START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT - 1
V3_BLOCK_HEIGHT = 10324749
V4_BLOCK_HEIGHT = 12640761


class DataFeeder(BaseDataFeeder):
    LAST_BLOCK_KEY = b'last_block_key'

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
        heightkey = BLOCK_HEIGHT_KEY + height.to_bytes(BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
        block_hash = db.get(heightkey)
        if verbose:
            print(f'--Time to find block hash: {round(time() - t11, 4)}s')

        if not block_hash:
            if verbose:
                print(f'--WARNING: block hash not found: {heightkey}')
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
            if verbose:
                print('--ERROR in block data loading, skipped feeding')
            return None

        if verbose:
            print(f'Total time to load data: {round(time() - t1, 4)}s')

        try:
            t4 = time()
            set_stake_wallets = {}
            for tx in txs:
                if 'data' not in tx:
                    continue
                if 'method' not in tx['data']:
                    continue
                if tx['data']['method'] == 'setStake':
                    try:
                        stake_value = int(tx['data']['params']['value'], 16) / 10 ** 18
                        set_stake_wallets[tx["from"]] = stake_value
                    except ValueError:
                        pass
            if verbose:
                print(f'Time to pre-process data: {round(time() - t4, 4)}s')

        except Exception as e:
            if verbose:
                print('ERROR in data pre-processing')
                print(e)
                print(traceback.format_exc())
            return None

        if verbose:
            print(f'Total feeding time: {round(time() - t1, 4)}s')
            print()

        return set_stake_wallets

    async def last_block_height(self) -> Optional[int]:
        """Get last block height from chain
        """
        db = self.chain_db

        block_hash = db.get(DataFeeder.LAST_BLOCK_KEY)
        data = db.get(block_hash)
        if data:
            block = json.loads(data)
            return int(block['height'], 16)
