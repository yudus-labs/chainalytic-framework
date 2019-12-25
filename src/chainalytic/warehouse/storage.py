from pathlib import Path
from pprint import pprint
from typing import Collection, Dict, List, Optional, Set, Tuple, Union

import plyvel

from chainalytic.common import config, zone_manager


class BaseStorage(object):
    """
    Base class for different Storage implementations

    Properties:
        working_dir (str):
        zone_id (str):
        warehouse_dir (str):
        zone_storage_dir (str):
        transform_storage_dirs (dict):
        transform_storage_dbs (dict):
    
    Methods:
        put_block(height: int, data: dict, transform_id: str) -> bool
        get_block(self, height: int, transform_id: str) -> Dict
        last_block_height(transform_id: str) -> int
        set_last_block_height(height: int, transform_id: str) -> bool

    """

    LAST_BLOCK_HEIGHT_KEY = b'last_block_height'

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseStorage, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id

        setting = config.get_setting(working_dir)
        self.warehouse_dir = Path(working_dir, setting['warehouse_dir']).as_posix()
        self.zone_storage_dir = setting['zone_storage_dir'].format(
            warehouse_dir=self.warehouse_dir, zone_id=zone_id,
        )

        transforms = zone_manager.load_zone(zone_id)['aggregator']['transform_registry']
        self.transform_storage_dirs = {
            tid: setting['transform_storage_dir'].format(
                zone_storage_dir=self.zone_storage_dir, transform_id=tid
            )
            for tid in transforms
        }

        # Setup storage DB for all transforms
        for p in self.transform_storage_dirs.values():
            Path(p).parent.mkdir(parents=1, exist_ok=1)
        self.transform_storage_dbs = {
            tid: plyvel.DB(self.transform_storage_dirs[tid], create_if_missing=True)
            for tid in transforms
        }

    async def put_block(
        self, height: int, data: Union[Collection, bytes, str, float, int], transform_id: str
    ) -> bool:
        """Put block data to one specific transform storage.
        
        `last_block_height` value is also updated here
        """
        return 1

    async def get_block(self, height: int, transform_id: str) -> Optional[str]:
        """Get block data from one specific transform storage."""
        return ''

    async def last_block_height(self, transform_id: str) -> int:
        """Get last block height in one specific transform storage."""
        return 1

    async def set_last_block_height(self, height: int, transform_id: str) -> bool:
        """Set last block height in one specific transform storage."""
        return 1
