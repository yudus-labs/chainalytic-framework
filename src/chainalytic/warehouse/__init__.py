from . import storage
from chainalytic.common import config


class Warehouse(object):
    def __init__(self, working_dir: str):
        super(Warehouse, self).__init__()
        self.setting = config.get_setting(working_dir)
        self.chain_registry = config.get_chain_registry(working_dir)
        self.working_dir = working_dir
