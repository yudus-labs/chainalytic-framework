import os
import pytest
import plyvel
import secrets
from chainalytic.common import trie


def test_trie(setup_temp_db):
    db_path = setup_temp_db
    db = plyvel.DB(db_path, create_if_missing=True)

    # Create new trie with random paths
    number_of_paths = 10
    t = trie.Trie()
    valid_data = {}
    for i in range(number_of_paths):
        p = secrets.token_hex(20)
        t.add_path(f'hx{p}:{i}')
        valid_data[f'hx{p}'] = i

    # Save to leveldb
    encoded_trie = t.to_bytes()
    db.put(b'test', encoded_trie)
    db.close()

    # Load Trie from leveldb
    t = trie.Trie()
    db = plyvel.DB(db_path, create_if_missing=True)
    t.from_bytes(db.get(b'test'))

    # Test path and value
    paths = t.ls_paths(skip_value=1)
    for p in valid_data:
        assert p in paths
        assert int(t.get_value(p)) == valid_data[p]

    values = [int(v) for v in t.ls_values()]
    for v in valid_data.values():
        assert v in values
