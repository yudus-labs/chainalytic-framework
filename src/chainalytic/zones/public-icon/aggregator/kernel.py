from typing import Any, Dict, List, Optional, Set, Tuple, Union

from chainalytic.aggregator.kernel import BaseKernel
from chainalytic.common import config, rpc_client


class Kernel(BaseKernel):
    FIRST_STAKE_BLOCK_HEIGHT = 7597365
    START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT - 1

    def __init__(self, working_dir: str, zone_id: str):
        super(Kernel, self).__init__(working_dir, zone_id)

    async def execute(self, height: int, input_data: Any, transform_id: str) -> Optional[bool]:
        """Execute transform and push output data to warehouse
        """
        output = await self.transforms[transform_id].execute(height, input_data)
        if not output:
            return 0

        if transform_id == 'stake_history':
            r = await rpc_client.call_async(
                self.warehouse_endpoint,
                call_id='api_call',
                api_id='put_block',
                api_params={
                    'height': output['height'],
                    'data': output['data'],
                    'transform_id': transform_id,
                },
            )
            r2 = await rpc_client.call_async(
                self.warehouse_endpoint,
                call_id='api_call',
                api_id='set_latest_unstake_state',
                api_params={
                    'unstake_state': output['misc']['latest_unstake_state'],
                    'transform_id': transform_id,
                },
            )
            return r['status'] and r2['status']
        elif transform_id == 'stake_top100':
            r = await rpc_client.call_async(
                self.warehouse_endpoint,
                call_id='api_call',
                api_id='set_latest_stake_top100',
                api_params={
                    'stake_top100': output['misc']['latest_stake_top100'],
                    'transform_id': transform_id,
                },
            )
            return r['status']

