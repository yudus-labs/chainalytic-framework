import json


def pretty(d: dict) -> str:
    return json.dumps(d, indent=2, sort_keys=1)
