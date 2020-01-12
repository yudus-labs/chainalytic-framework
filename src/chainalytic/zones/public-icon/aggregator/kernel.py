from typing import Any, Dict, List, Optional, Set, Tuple, Union

from chainalytic.aggregator.kernel import BaseKernel
from chainalytic.common import config, rpc_client


class Kernel(BaseKernel):
    def __init__(self, working_dir: str, zone_id: str):
        super(Kernel, self).__init__(working_dir, zone_id)

    async def execute(self, height: int, input_data: Any, transform_id: str) -> Optional[bool]:
        """Execute transform and push output data to warehouse
        """
        output = None
        if transform_id in self.transforms:
            try:
                output = await self.transforms[transform_id].execute(height, input_data)
            except Exception as e:
                self.logger.error(f'ERROR while executing transform {transform_id}')
                self.logger.error(str(e))
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
        elif transform_id == 'recent_stake_wallets':
            r = await rpc_client.call_async(
                self.warehouse_endpoint,
                call_id='api_call',
                api_id='set_recent_stake_wallets',
                api_params={
                    'recent_stake_wallets': output['misc']['recent_stake_wallets'],
                    'transform_id': transform_id,
                },
            )
            return r['status']
        elif transform_id == 'abstention_stake':
            r = await rpc_client.call_async(
                self.warehouse_endpoint,
                call_id='api_call',
                api_id='set_abstention_stake',
                api_params={
                    'abstention_stake': output['misc']['abstention_stake'],
                    'transform_id': transform_id,
                },
            )
            return r['status']
        elif transform_id == 'funded_wallets':
            r = await rpc_client.call_async(
                self.warehouse_endpoint,
                call_id='api_call',
                api_id='update_funded_wallets',
                api_params={
                    'updated_wallets': output['misc']['updated_wallets'],
                    'transform_id': transform_id,
                },
            )
            return r['status']

