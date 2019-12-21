from typing import List, Set, Dict, Tuple, Optional, Union
from chainalytic.common import config, rpc_client
from chainalytic.provider.collator import BaseCollator


class Collator(BaseCollator):

    def __init__(self, working_dir: str, zone_id: str):
        super(Collator, self).__init__(working_dir, zone_id)

    async def get_block(
        self, height: int, transform_id: str
    ) -> Optional[Union[Dict, str, float, int, bytes]]:
        r = await rpc_client.call_async(
            self.warehouse_endpoint,
            call_id='get_block',
            height=height,
            transform_id=transform_id
        )
        if r['status'] and r['data']:
            return r['data']
        else:
            return None
