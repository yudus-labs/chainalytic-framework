from typing import Dict, List, Optional, Set, Tuple

from chainalytic.common import config, zone_manager

from . import api_bundle, collator


class Provider(object):
    """
    Properties:
        working_dir (str):
        zone_id (str):
        setting (dict):
        chain_registry (dict):
        collator (Collator):
        api_bundle (ApiBundle):

    """

    def __init__(self, working_dir: str, zone_id: str):
        super(Provider, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id

        config.set_working_dir(working_dir)
        self.setting = config.get_setting(working_dir)
        self.chain_registry = config.get_chain_registry(working_dir)

        mods = zone_manager.load_zone(self.zone_id, working_dir)['provider']
        self.collator = mods['collator'].Collator(working_dir, zone_id)
        self.api_bundle = mods['api_bundle'].ApiBundle(working_dir, zone_id)
        self.api_bundle.set_collator(self.collator)
