from pathlib import Path
from typing import Collection, Dict, List, Optional, Set, Tuple

import plyvel

from chainalytic.common import config, zone_manager
from chainalytic.common.util import get_child_logger


class BaseDataFeeder(object):
    """
    Base class for different Data Feeder implementations

    Properties:
        working_dir (str):
        zone_id (str):
        zone (dict):
        direct_db_access (bool):

    Methods:
        get_block(height: int) -> Optional[Dict]

    """

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseDataFeeder, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        self.zone = zone_manager.get_zone(working_dir, zone_id)
        self.direct_db_access = self.zone['direct_db_access']

        self.logger = get_child_logger('upstream.data_feeder')

    async def get_block(self, height: int, transform_id: str) -> Optional[Collection]:
        """Retrieve standard block data from chain
        """
        block = {}
        return block

    async def last_block_height(self) -> Optional[int]:
        """Get last block height from chain
        """
        return 1
