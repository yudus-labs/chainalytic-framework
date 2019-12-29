from typing import Dict, List, Optional, Set, Tuple, Union

from chainalytic.common import config, zone_manager


class BaseCollator(object):
    """
    Base class for different Collator implementations

    Properties:
        working_dir (str):
        zone_id (str):
        warehouse_endpoint (str):

    Methods:
        None

    """

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseCollator, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        self.warehouse_endpoint = config.get_setting(working_dir)['warehouse_endpoint']

