from typing import List, Set, Dict, Tuple, Optional, Any, Callable
from chainalytic.common import config
from chainalytic.provider.api_bundle import BaseApiBundle


class ApiBundle(BaseApiBundle):

    def __init__(self, working_dir: str, zone_id: str):
        super(ApiBundle, self).__init__(working_dir, zone_id)

    async def get_staking_info(self, api_params: dict) -> Optional[dict]:
        if 'height' in api_params:
            return await self.collator.get_block(api_params['height'], 'stake_history')

    async def last_block_height(self, api_params: dict) -> Optional[int]:
        if 'transform_id' in api_params:
            return await self.collator.last_block_height(api_params['transform_id'])

    async def get_staking_info_last_block(self, api_params: dict) -> Optional[Dict]:
        if 'transform_id' in api_params:
            height = await self.collator.last_block_height(api_params['transform_id'])
            if height:
                return await self.collator.get_block(height, 'stake_history')
