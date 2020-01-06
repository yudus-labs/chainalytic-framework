"""
Exposed APIs

from chainalytic.common import rpc_client

rpc_client.call_aiohttp(
    'localhost:5530',
    call_id='api_call',
    api_id='get_staking_info',
    api_params={'height': 9999999}
)
rpc_client.call_aiohttp(
    'localhost:5530',
    call_id='api_call',
    api_id='last_block_height',
    api_params={'transform_id': 'stake_history'}
)
rpc_client.call_aiohttp(
    'localhost:5530',
    call_id='api_call',
    api_id='get_staking_info_last_block',
    api_params={}
)
rpc_client.call_aiohttp(
    'localhost:5530',
    call_id='api_call',
    api_id='latest_unstake_state',
    api_params={}
)
rpc_client.call_aiohttp(
    'localhost:5530',
    call_id='api_call',
    api_id='latest_stake_top100',
    api_params={}
)

"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chainalytic.common import config
from chainalytic.provider.api_bundle import BaseApiBundle


class ApiBundle(BaseApiBundle):
    """
    The interface to external consumers/applications
    """

    def __init__(self, working_dir: str, zone_id: str):
        super(ApiBundle, self).__init__(working_dir, zone_id)

    async def get_staking_info(self, api_params: dict) -> Optional[dict]:
        if 'height' in api_params:
            return await self.collator.get_block(api_params['height'], 'stake_history')

    async def last_block_height(self, api_params: dict) -> Optional[int]:
        if 'transform_id' in api_params:
            return await self.collator.last_block_height(api_params['transform_id'])

    async def get_staking_info_last_block(self, api_params: dict) -> Optional[Dict]:
        height = await self.collator.last_block_height('stake_history')
        if height:
            r = await self.collator.get_block(height, 'stake_history')
            if r:
                r['height'] = height
                return r

    async def latest_unstake_state(self, api_params: dict) -> Optional[int]:
        return await self.collator.latest_unstake_state('stake_history')

    async def latest_stake_top100(self, api_params: dict) -> Optional[dict]:
        return await self.collator.latest_stake_top100('stake_top100')

    async def recent_stake_wallets(self, api_params: dict) -> Optional[dict]:
        return await self.collator.recent_stake_wallets('recent_stake_wallets')

