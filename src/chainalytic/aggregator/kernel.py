from typing import List, Set, Dict, Tuple, Optional, Any, Callable
from chainalytic.common import config, rpc_client


class BaseKernel(object):
    """
    Base class for different Kernel implementations

    Properties:
        working_dir (str):
        zone_id (str):
        transforms (dict):
        warehouse_endpoint (str):

    Methods:
        add_transform(transform: Transform)
        execute(height: int, input_data: Dict, transform_id: str)
    """

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseKernel, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        self.transforms = {}

        self.warehouse_endpoint = config.get_setting(working_dir)['warehouse_endpoint']

    def add_transform(self, transform: 'Transform'):
        self.transforms[transform.transform_id] = transform
        transform.set_kernel(self)

    async def execute(self, height: int, input_data: Any, transform_id: str) -> Optional[bool]:
        """Execute transform and push output data to warehouse
        """
        if transform_id in self.transforms:
            output = await self.transforms[transform_id].execute(height, input_data)
            r = await rpc_client.call_async(
                self.warehouse_endpoint,
                call_id='put_block',
                height=height,
                data=output['data'],
                transform_id=transform_id,
            )
            return r['status']
