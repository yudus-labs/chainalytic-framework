import json
from typing import Dict, List, Optional, Set, Tuple, Union

from chainalytic.common import config, rpc_client
from chainalytic.provider.collator import BaseCollator


class Collator(BaseCollator):
    def __init__(self, working_dir: str, zone_id: str):
        super(Collator, self).__init__(working_dir, zone_id)

    async def get_block(
        self, height: int, transform_id: str
    ) -> Optional[Union[Dict, str, float, int, bytes]]:
        r = await rpc_client.call_async(
            self.warehouse_endpoint, call_id='get_block', height=height, transform_id=transform_id
        )
        if r['status'] and r['data']:
            try:
                return json.loads(r['data'])
            except:
                return None
        else:
            return None

    async def last_block_height(self, transform_id: str) -> Optional[int]:
        r = await rpc_client.call_async(
            self.warehouse_endpoint, call_id='last_block_height', transform_id=transform_id
        )
        if r['status'] and r['data']:
            try:
                return int(r['data'])
            except:
                return None
        else:
            return None
