from typing import Dict, List, Optional, Set, Tuple, Union

from chainalytic.aggregator.kernel import BaseKernel
from chainalytic.common import config


class Kernel(BaseKernel):
    FIRST_STAKE_BLOCK_HEIGHT = 7597365
    START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT - 1

    def __init__(self, working_dir: str, zone_id: str):
        super(Kernel, self).__init__(working_dir, zone_id)

