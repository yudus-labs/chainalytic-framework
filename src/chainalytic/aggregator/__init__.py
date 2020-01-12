from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chainalytic.common import config, zone_manager

from . import kernel, transform


class Aggregator(object):
    """
    Properties:
        working_dir (str):
        zone_id (str):
        setting (dict):
        chain_registry (dict):
        kernel (Kernel):
        upstream_endpoint (str):
        warehouse_endpoint (str):

    """

    def __init__(self, working_dir: str, zone_id: str):
        super(Aggregator, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id

        config.set_working_dir(working_dir)
        self.setting = config.get_setting(working_dir)
        self.chain_registry = config.get_chain_registry(working_dir)

        mods = zone_manager.load_zone(self.zone_id, working_dir)['aggregator']
        self.kernel = mods['kernel'].Kernel(working_dir, zone_id)

        for transform_id in mods['transform_registry']:
            t = mods['transform_registry'][transform_id].Transform(
                working_dir, zone_id, transform_id
            )
            self.kernel.add_transform(t)

        self.upstream_endpoint = config.get_setting(working_dir)['upstream_endpoint']
        self.warehouse_endpoint = config.get_setting(working_dir)['warehouse_endpoint']
