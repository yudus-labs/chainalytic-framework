from typing import Dict, List, Optional, Set, Tuple

from chainalytic.common import config, zone_manager

from . import storage


class Warehouse(object):
    """
    Properties:
        working_dir (str):
        zone_id (str):
        setting (dict):
        chain_registry (dict):
        storage (Storage):

    """

    def __init__(self, working_dir: str, zone_id: str):
        super(Warehouse, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id

        config.set_working_dir(working_dir)
        self.setting = config.get_setting(working_dir)
        self.chain_registry = config.get_chain_registry(working_dir)

        mods = zone_manager.load_zone(self.zone_id, working_dir)['warehouse']
        self.storage = mods['storage'].Storage(working_dir, zone_id)
