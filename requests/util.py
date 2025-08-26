import json

def dict_to_canonical_json(d: dict) -> str:
    return json.dumps(d, sort_keys=True, separators=(',',':'))

def as_key(d: dict) -> str:
    return dict_to_canonical_json(d)

def sanitize_keys(d: dict, keys: list[str]):
    sanitized = {
        key: d.get(key) for key in keys
        if key in d
    }
    return sanitized