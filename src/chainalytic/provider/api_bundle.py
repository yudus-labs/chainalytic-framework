import traceback
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chainalytic.common import config, zone_manager

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
        register_api(api_id, func: Callable)
        api_call(api_id: str, params: dict) -> Dict
 
    """

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseApiBundle, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        self.collator = None

    def set_collator(self, collator: BaseCollator):
        self.collator = collator

    async def api_call(self, api_id: str, api_params: dict) -> Dict:
        ret = {'status': 0, 'result': None}
        func = getattr(self, api_id) if hasattr(self, api_id) else None

        try:
            if func:
                ret['result'] = await func(api_params)
                ret['status'] = 1
            else:
                ret['status'] = -1
        except Exception as e:
            ret['status'] = 0
            ret['result'] = f'{str(e)}\n{traceback.format_exc()}'

        return ret
