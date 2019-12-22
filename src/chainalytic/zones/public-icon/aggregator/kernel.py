from typing import List, Set, Dict, Tuple, Optional, Union
from chainalytic.aggregator.kernel import BaseKernel


class Kernel(BaseKernel):

    FIRST_STAKE_BLOCK_HEIGHT = 7597365
    START_BLOCK_HEIGHT = FIRST_STAKE_BLOCK_HEIGHT - 1
    V3_BLOCK_HEIGHT = 10324749

    def __init__(self, working_dir: str, zone_id: str):
        super(Kernel, self).__init__(working_dir, zone_id)
