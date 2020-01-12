from pprint import pprint
from typing import Dict, List, Optional, Set, Tuple

import msgpack



class Trie(list):
    """
    A simple trie (radix tree) for different kinds of state representation

    Methods:
        add_path(full_path: str)
        ls_values(verbose=0) -> List[str]
        get_value(path: str) -> Optional[str]
        ls_paths(skip_value=1, verbose=0) -> List[str]
        to_bytes()-> bytes
        from_bytes(encoded_trie: bytes)
        to_hex()-> str
        from_hex(encoded_trie: str)
        render()
    """

    ADDRESS_SIZE = 40
    PREFIX = 'hx'

    def __init__(self):
        super().__init__()
        self.extend([''] * 16)

    def add_path(self, full_path: str):
        """Add address full_path to `Trie`

        Full path format: `<2_chars_prefix><address_body>:<arbitrary_value>`
        """
        path = full_path[len(Trie.PREFIX) :]

        def walk(node: list, depth: int):
            # print(f'Cur node: {node}')
            # print(f'Depth: {depth}')
            entry = int(path[depth], 16)
            if isinstance(node[entry], list):
                walk(node[entry], depth + 1)
            elif isinstance(node[entry], (str, bytes)):
                if depth + 1 == Trie.ADDRESS_SIZE:
                    node[entry] = path[Trie.ADDRESS_SIZE + 1 :]
                else:
                    node[entry] = [''] * 16
                    walk(node[entry], depth + 1)

        walk(self, 0)

    def ls_values(self, verbose=0) -> List[str]:
        """List all values associated with paths in Trie

        Returns:
            list: list of values
        """
        values = []

        def walk(node: list, depth: int):
            for c in node:
                if isinstance(c, list):
                    walk(c, depth + 1)
                elif depth + 1 == Trie.ADDRESS_SIZE and c:
                    values.append(c)

        walk(self, 0)
        if verbose:
            print(f'Total values: {len(values)}')
            pprint(values)
        return values

    def get_value(self, path: str) -> Optional[str]:
        """Find associated value of one specific path in Trie

        Path format: `<2_chars_prefix><address_body>`
        Return `None` if the path is not in trie
        """
        path = path[len(Trie.PREFIX) :]

        def walk(node: list, depth: int):
            entry = int(path[depth], 16)
            if isinstance(node[entry], list):
                return walk(node[entry], depth + 1)
            elif depth + 1 == Trie.ADDRESS_SIZE and node:
                return node[entry]

        return walk(self, 0)

    def ls_paths(self, skip_value=1, verbose=0) -> List[str]:
        """List all full paths in Trie

        Returns:
            list: list of paths
        """
        paths = []

        def walk(node: list, depth: int, cur_path: str):
            for i, c in enumerate(node):
                if isinstance(c, list):
                    walk(c, depth + 1, cur_path + hex(i)[2:])
                elif depth + 1 == Trie.ADDRESS_SIZE and c:
                    val = c.decode() if isinstance(c, bytes) else c
                    val = '' if skip_value else ':' + val
                    paths.append(f'{cur_path + hex(i)[2:]}{val}')

        walk(self, 0, Trie.PREFIX)
        if verbose:
            print(f'Total paths: {len(paths)}')
            pprint(paths)
        return paths

    def to_bytes(self) -> bytes:
        """Serialize the Trie using `MessagePack` protocol.

        Returns:
            bytes: encoded Trie
        """
        return msgpack.dumps(self)

    def from_bytes(self, encoded_trie: bytes):
        """Deserialize and restore Trie from bytes encoded by `MessagePack`
        """
        try:
            unpacked_data = msgpack.loads(encoded_trie)
        except Exception:
            raise Exception('Invalid encoded trie data')

        if len(unpacked_data) != 16:
            raise Exception('Invalid encoded trie data')
        for i, v in enumerate(unpacked_data):
            self[i] = v

    def to_hex(self) -> str:
        return self.to_bytes().hex()

    def from_hex(self, encoded_trie: str):
        try:
            unpacked_data = msgpack.loads(bytes.fromhex(encoded_trie))
        except Exception:
            raise Exception('Invalid encoded trie data')

        if len(unpacked_data) != 16:
            raise Exception('Invalid encoded trie data')
        for i, v in enumerate(unpacked_data):
            self[i] = v

    def render(self):
        def walk(node: list, depth: int):
            for i, c in enumerate(node):
                if isinstance(c, list):
                    print(f'{depth*"--"}{hex(i)[2:]}')
                    walk(c, depth + 1)
                elif isinstance(c, (str, bytes)) and c != '':
                    print(f'{depth*"--"}{hex(i)[2:]}:{c}')

        walk(self, 0)
