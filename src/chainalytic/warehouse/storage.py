import traceback
from pathlib import Path
from pprint import pprint
from typing import Any, Collection, Dict, List, Optional, Set, Tuple, Union

import plyvel

from chainalytic.common import config, zone_manager
from chainalytic.common.util import get_child_logger


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
        api_call(api_id: str, api_params: dict) -> Optional[Any]

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

        transforms = zone_manager.load_zone(zone_id, working_dir)['aggregator'][
            'transform_registry'
        ]
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

        self.logger = get_child_logger('warehouse.storage')

    async def api_call(self, api_id: str, api_params: dict) -> Optional[Any]:
        func = getattr(self, api_id) if hasattr(self, api_id) else None

        if func:
            try:
                return await func(api_params)
            except Exception as e:
                self.logger.info(f'{str(e)} \n {traceback.format_exc()}')
                return None
        else:
            self.logger.info(f'Storage API not implemented: {api_id}')
            return None
