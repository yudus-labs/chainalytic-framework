from . import collator
from . import api_bundle
from chainalytic.common import config


class Provider(object):
    def __init__(self, working_dir: str):
        super(Provider, self).__init__()
        self.setting = config.get_setting(working_dir)
        self.chain_registry = config.get_chain_registry(working_dir)
        self.working_dir = working_dir
