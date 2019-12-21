from typing import List, Set, Dict, Tuple, Optional
from chainalytic.common import config, zone_manager


class BaseDataFeeder(object):
    """
    Base class for different Data Feeder implementations

    Properties:
        working_dir (str):
        zone_id (str):
        client_endpoint (str):
        chain_db_dir (str):
    
    Methods:
        get_block(height: int) -> Optional[Dict]

    """

    def __init__(self, working_dir: str, zone_id: str):
        super(BaseDataFeeder, self).__init__()
        self.working_dir = working_dir
        self.zone_id = zone_id
        zone = zone_manager.get_zone(working_dir, zone_id)
        self.client_endpoint = zone['client_endpoint'] if zone else ''
        self.chain_db_dir = zone['chain_db_dir'] if zone else ''

    async def get_block(self, height: int) -> Optional[Dict]:
        """Retrieve standard block data from chain
        """
        block = {}
        return block
