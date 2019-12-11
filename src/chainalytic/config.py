from pathlib import Path
from ruamel.yaml import YAML

_CHAIN_REGISTRY_PATH = Path(__file__).resolve().parent.joinpath('config', 'chain_registry.yml').as_posix()
_SETTING_PATH = Path(__file__).resolve().parent.joinpath('config', 'setting.yml').as_posix()

CHAIN_REGISTRY = None
SETTING = None

with open(_CHAIN_REGISTRY_PATH) as f:
    yaml = YAML(typ='safe')
    CHAIN_REGISTRY = yaml.load(f.read())

with open(_SETTING_PATH) as f:
    yaml = YAML(typ='safe')
    SETTING = yaml.load(f.read())
