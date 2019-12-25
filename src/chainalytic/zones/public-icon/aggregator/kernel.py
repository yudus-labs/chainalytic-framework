from typing import Dict, List, Optional, Set, Tuple, Union

from chainalytic.aggregator.kernel import BaseKernel
from chainalytic.common import config


class Kernel(BaseKernel):

    FIRST_STAKE_BLOCK_HEIGHT = 7597365
    START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT - 1
    V3_BLOCK_HEIGHT = 10324749
    V4_BLOCK_HEIGHT = 12640761

    def __init__(self, working_dir: str, zone_id: str):
        super(Kernel, self).__init__(working_dir, zone_id)

        self.score_db_icondex_dir = None
        for zone in self.chain_registry['zones']:
            if zone['zone_id'] == zone_id:
                self.score_db_icondex_dir = zone['score_db_icondex_dir']
                break
