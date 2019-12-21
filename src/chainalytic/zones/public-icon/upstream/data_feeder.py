from typing import List, Set, Dict, Tuple, Optional
import json
from chainalytic.common import config, zone_manager
from chainalytic.upstream.data_feeder import BaseDataFeeder
import plyvel

BLOCK_HEIGHT_KEY = b'block_height_key'
BLOCK_HEIGHT_BYTES_LEN = 12


class DataFeeder(BaseDataFeeder):

    def __init__(self, working_dir: str, zone_id: str):
        super(DataFeeder, self).__init__(working_dir, zone_id)

    async def get_block(self, height: int) -> Optional[Dict]:
        """Retrieve standard block data from chain
        """
        if self.chain_db_dir:
            db = plyvel.DB(self.chain_db_dir)
            block_hash = db.get(
                BLOCK_HEIGHT_KEY +
                height.to_bytes(BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
            )
            if not block_hash:
                return None
            block = json.loads(db.get(block_hash))
            db.close()

            return block
