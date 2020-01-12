from typing import Dict, List, Optional, Set, Tuple

from chainalytic.common import config, zone_manager

from . import data_feeder


class Upstream(object):
    """
    Properties:
        working_dir (str):
        zone_id (str):
        setting (dict):
        chain_registry (dict):
        data_feeder (DataFeeder):

    """
    def __init__(self, working_dir: str, zone_id: str):
        super(Upstream, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id

        config.set_working_dir(working_dir)
        self.setting = config.get_setting(working_dir)
        self.chain_registry = config.get_chain_registry(working_dir)

        mods = zone_manager.load_zone(self.zone_id, working_dir)['upstream']
        self.data_feeder = mods['data_feeder'].DataFeeder(working_dir, zone_id)
