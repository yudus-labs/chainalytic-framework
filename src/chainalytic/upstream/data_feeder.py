from pathlib import Path
from typing import Collection, Dict, List, Optional, Set, Tuple

import plyvel

from chainalytic.common import config, zone_manager


class BaseDataFeeder(object):
    """
    Base class for different Data Feeder implementations

    Properties:
        working_dir (str):
        zone_id (str):
        client_endpoint (str):
        chain_db_dir (str):
        chain_db (plyvel.DB):
    
    Methods:
        get_block(height: int) -> Optional[Dict]

    """

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseDataFeeder, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        zone = zone_manager.get_zone(working_dir, zone_id)
        self.client_endpoint = zone['client_endpoint'] if zone else ''
        self.chain_db_dir = zone['chain_db_dir'] if zone else ''

        assert Path(self.chain_db_dir).exists(), f'Chain DB does not exist: {self.chain_db_dir}'
        self.chain_db = plyvel.DB(self.chain_db_dir)

    async def get_block(self, height: int) -> Optional[Collection]:
        """Retrieve standard block data from chain
        """
        block = {}
        return block

    async def last_block_height(self) -> Optional[int]:
        """Get last block height from chain
        """
        return 1
