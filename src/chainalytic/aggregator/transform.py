from typing import List, Set, Dict, Tuple, Optional, Any, Callable
from pathlib import Path
import plyvel
from chainalytic.common import config


class BaseTransform(object):
    """
    Base class for different Transform implementations

    Properties:
        working_dir (str):
        zone_id (str):
        transform_id (str):
        warehouse_endpoint (str):
        kernel (Kernel):
        transform_storage_dir (str):
        transform_cache_dir (str):
        transform_cache_db (plyvel.DB):

    Methods:
        execute(height: int, input_data: Dict) -> Dict

    """

    def __init__(self, working_dir: str, zone_id: str, transform_id: str):
        super(BaseTransform, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        self.transform_id = transform_id
        self.kernel = None

        self.warehouse_endpoint = config.get_setting(working_dir)['warehouse_endpoint']

        setting = config.get_setting(working_dir)
        warehouse_dir = Path(working_dir, setting['warehouse_dir']).as_posix()
        zone_storage_dir = setting['zone_storage_dir'].format(
            warehouse_dir=warehouse_dir, zone_id=zone_id,
        )
        self.transform_storage_dir = setting['transform_storage_dir'].format(
            zone_storage_dir=zone_storage_dir, transform_id=transform_id
        )
        self.transform_cache_dir = setting['transform_cache_dir'].format(
            zone_storage_dir=zone_storage_dir, transform_id=transform_id
        )

        Path(self.transform_cache_dir).parent.mkdir(parents=1, exist_ok=1)
        self.transform_cache_db = plyvel.DB(self.transform_cache_dir, create_if_missing=True)

    def set_kernel(self, kernel: 'Kernel'):
        self.kernel = kernel

    async def execute(self, height: int, input_data: Any) -> Dict:
        return {'height': height, 'data': {}}

