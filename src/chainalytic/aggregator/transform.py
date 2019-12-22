from typing import List, Set, Dict, Tuple, Optional, Any, Callable
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

    Methods:
        execute(height: int, input_data: Dict) -> Dict

    """

    def __init__(self, working_dir: str, zone_id: str, transform_id: str):
        super(BaseTransform, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        self.transform_id = transform_id

        self.warehouse_endpoint = config.get_setting(working_dir)['warehouse_endpoint']
        self.kernel = None

    def set_kernel(self, kernel: 'Kernel'):
        self.kernel = kernel

    async def execute(self, height: int, input_data: Any) -> Dict:
        return {'height': height, 'data': {}}

