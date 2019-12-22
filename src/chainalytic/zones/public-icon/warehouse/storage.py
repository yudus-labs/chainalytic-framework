from typing import List, Set, Dict, Tuple, Optional, Union, Collection
from pathlib import Path
import json
import plyvel
from chainalytic.common import config, zone_manager
from chainalytic.warehouse.storage import BaseStorage


class Storage(BaseStorage):

    def __init__(self, working_dir: str, zone_id: str):
        super(Storage, self).__init__(working_dir, zone_id)

    async def put_block(
        self, height: int, data: Union[Collection, bytes, str, float, int], transform_id: str
    ) -> bool:
        """Put block data to one specific transform storage.

        `last_block_height` value is also updated here
        """

        transform_storage_dir = self.transform_storage_dirs[transform_id]
        if transform_storage_dir:
            Path(transform_storage_dir).parent.mkdir(parents=1, exist_ok=1)

            db = plyvel.DB(transform_storage_dir, create_if_missing=True)
            key = str(height).encode()

            if isinstance(data, dict):
                value = json.dumps(data).encode()
            elif isinstance(data, (list, tuple)):
                value = str(data).encode()
            elif isinstance(data, str):
                value = data.encode()
            elif isinstance(data, (int, float)):
                value = str(data).encode()
            elif isinstance(data, bytes):
                value = data
            else:
                return 0

            db.put(key, value)
            db.put(Storage.LAST_BLOCK_HEIGHT_KEY, key)
            db.close()
            return 1

    async def get_block(self, height: int, transform_id: str) -> Optional[str]:
        """Get block data from one specific transform storage."""

        transform_storage_dir = self.transform_storage_dirs[transform_id]
        if transform_storage_dir:
            if not Path(transform_storage_dir).exists():
                return None

            db = plyvel.DB(transform_storage_dir, create_if_missing=True)
            key = str(height).encode()
            value = db.get(key)
            value = value.decode() if value else value
            db.close()

            return value

    async def last_block_height(self, transform_id: str) -> Optional[int]:
        """Get last block height in one specific transform storage."""

        transform_storage_dir = self.transform_storage_dirs[transform_id]
        if transform_storage_dir:
            if not Path(transform_storage_dir).exists():
                return None

            db = plyvel.DB(transform_storage_dir, create_if_missing=True)
            value = db.get(Storage.LAST_BLOCK_HEIGHT_KEY)
            try:
                height = int(value)
            except Exception:
                height = None
            db.close()

            return height

    async def set_last_block_height(self, height: int, transform_id: str) -> bool:
        """Set last block height in one specific transform storage."""

        transform_storage_dir = self.transform_storage_dirs[transform_id]
        if transform_storage_dir:
            try:
                height = int(height)
                value = str(height).encode()
            except Exception:
                return 0

            Path(transform_storage_dir).parent.mkdir(parents=1, exist_ok=1)

            db = plyvel.DB(transform_storage_dir, create_if_missing=True)
            db.put(Storage.LAST_BLOCK_HEIGHT_KEY, value)

            db.close()

            return 1
