from . import data_feeder
from . import kernel
from . import transform
from chainalytic.common import config


class Aggregator(object):
    def __init__(self, working_dir: str):
        super(Aggregator, self).__init__()
        self.setting = config.get_setting(working_dir)
        self.chain_registry = config.get_chain_registry(working_dir)
        self.working_dir = working_dir
