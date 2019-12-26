import json
import traceback
from pathlib import Path
from pprint import pprint
from time import time
from typing import Dict, List, Optional, Set, Tuple

import plyvel
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider

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

        self.client_endpoint = self.zone['client_endpoint'] if self.zone else ''
        self.chain_db_dir = self.zone['chain_db_dir'] if self.zone else ''
        self.score_db_icondex_dir = self.zone['score_db_icondex_dir'] if self.zone else ''

        if self.direct_db_access:
            assert Path(self.chain_db_dir).exists(), f'Chain DB does not exist: {self.chain_db_dir}'
            self.chain_db = plyvel.DB(self.chain_db_dir)
            self.score_db_icondex_db = plyvel.DB(self.score_db_icondex_dir)
        else:
            self.icon_service = IconService(HTTPProvider(f"http://{self.client_endpoint}", 3))
            self.icon_service.get_total_supply()

    def _get_total_supply(self):
        if self.direct_db_access:
            r = self.score_db_icondex_db.get(b'total_supply')
            return int.from_bytes(r, 'big') / 10 ** 18
        else:
            return self.icon_service.get_total_supply() / 10 ** 18

    def _get_block(self, height: int) -> Optional[Dict]:
        if self.direct_db_access:
            heightkey = BLOCK_HEIGHT_KEY + height.to_bytes(BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
            block_hash = self.chain_db.get(heightkey)

            if not block_hash:
                return None

            data = self.chain_db.get(block_hash)
            try:
                return json.loads(data)
            except:
                return None
        else:
            try:
                return self.icon_service.get_block(height)
            except:
                return None

    async def get_block(self, height: int, verbose: bool = 0) -> Optional[dict]:
        """Retrieve standard block data from chain
        """
        if verbose:
            print(f'Feeding block: {height}')

        block = self._get_block(height)
        if not block:
            return None

        try:
            if height < V3_BLOCK_HEIGHT or not self.direct_db_access:
                txs = block['confirmed_transaction_list']
                timestamp = block['time_stamp']
            else:
                txs = block['transactions']
                timestamp = int(block['timestamp'], 16)

        except Exception:
            if verbose:
                print('--ERROR in block data loading, skipped feeding')
            return None

        try:
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
        except Exception as e:
            if verbose:
                print('ERROR in data pre-processing')
                print(e)
                print(traceback.format_exc())
            return None

        return {
            'data': set_stake_wallets,
            'timestamp': timestamp,
            'total_supply': self._get_total_supply(),
        }

    async def last_block_height(self) -> Optional[int]:
        """Get last block height from chain
        """

        if self.direct_db_access:
            block_hash = self.chain_db.get(DataFeeder.LAST_BLOCK_KEY)
            data = self.chain_db.get(block_hash)
            if data:
                block = json.loads(data)
                return int(block['height'], 16)
        else:
            return self.icon_service.get_block('latest')['height']
