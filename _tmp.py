import sys
from pathlib import Path
from pprint import pprint
import secrets
import timeit
import plyvel
from chainalytic.common import trie


def measure_time(func):
    print(f'Time: {round(timeit.timeit(func, number=1), 4)}s')


def trie_tmp():
    t = trie.Trie()
    for i in range(10):
        p = secrets.token_hex(20)
        t.add_path(f'hx{p}:{i}')

    db_path = Path(__file__).resolve().parent.joinpath('_tmp', 'TMP_DB')
    db_path.mkdir(parents=1, exist_ok=1)

    encoded_trie = t.encode()
    db = plyvel.DB(db_path.as_posix(), create_if_missing=True)
    db.put(b'test', encoded_trie)
    db.close()

    t = trie.Trie()
    db = plyvel.DB(db_path.as_posix(), create_if_missing=True)
    t.decode(db.get(b'test'))

    t.ls_values()
    paths = t.ls_paths(verbose=1)
    p = paths[2].split(':')[0]
    print(f'Value of {p}')
    print(t.get_value(p))

    print('Finished trie_tmp()!')


if __name__ == "__main__":
    measure_time(trie_tmp)
