import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chainalytic.common import config, zone_manager
from chainalytic.common.util import get_child_logger

from .collator import BaseCollator


class BaseApiBundle(object):
    """
    Base class for different Api Bundle implementations

    Properties:
        working_dir (str):
        zone_id (str):
        all_api (dict):
        collator (BaseCollator):

    Methods:
        set_collator(collator: BaseCollator)
        api_call(api_id: str, api_params: dict) -> Dict
 
    """

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseApiBundle, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        self.collator = None

        self.logger = get_child_logger('provider.api_bundle')

    def set_collator(self, collator: BaseCollator):
        self.collator = collator

    async def api_call(self, api_id: str, api_params: dict) -> Dict:
        ret = {'status': 0, 'result': None}
        func = getattr(self, api_id) if hasattr(self, api_id) else None

        try:
            if func:
                self.logger.debug(f'Found API: {api_id}, calling...')
                ret['result'] = await func(api_params)
                ret['status'] = 1
            else:
                self.logger.warning(f'API not found: {api_id}')
                ret['status'] = -1
                ret['result'] = f'API not found: {api_id}'
        except Exception as e:
            ret['status'] = 0
            ret['result'] = f'{str(e)}\n{traceback.format_exc()}'
            self.logger.error(f'ERROR when calling API: {api_id}')
            self.logger.error(f'{str(e)}\n{traceback.format_exc()}')

        return ret
