import importlib
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from chainalytic.common import config

ZONE_MODULES = {
    'upstream': {'data_feeder': 'data_feeder.py'},
    'aggregator': {'kernel': 'kernel.py'},
    'warehouse': {'storage': 'storage.py'},
    'provider': {'api_bundle': 'api_bundle.py', 'collator': 'collator.py'},
}


def load_zone(zone_id: str, working_dir: str) -> Dict[str, Dict]:
    """Load service-modules of a specific zone
    """
    zone = get_zone(working_dir, zone_id)
    zone = zone if zone else {}
    transforms = zone['transforms'] if 'transforms' in zone else []

    ret = {
        'upstream': {'data_feeder': None},
        'aggregator': {'kernel': None, 'transform_registry': {}},
        'warehouse': {'storage': None},
        'provider': {'api_bundle': None, 'collator': None},
    }
    zone_implementation_dir = Path(__file__).resolve().parent.parent.joinpath('zones', zone_id)
    for service in ZONE_MODULES:
        for mod in ZONE_MODULES[service]:
            spec = importlib.util.spec_from_file_location(
                mod,
                zone_implementation_dir.joinpath(service, ZONE_MODULES[service][mod]).as_posix(),
            )
            module = importlib.util.module_from_spec(spec)
            ret[service][mod] = module
            spec.loader.exec_module(module)

    for p in zone_implementation_dir.joinpath('aggregator', 'transform_registry').glob('[!^_]*.py'):
        if p.stem not in transforms and transforms:
            continue
        spec = importlib.util.spec_from_file_location(p.name, p.as_posix())
        module = importlib.util.module_from_spec(spec)
        ret['aggregator']['transform_registry'][p.stem] = module
        spec.loader.exec_module(module)

    return ret


def get_zone(working_dir: str, zone_id: str) -> Optional[Dict]:
    for zone in config.get_chain_registry(working_dir)['zones']:
        if zone_id == zone['zone_id']:
            return zone
