from typing import *
from pathlib import Path
import json

import click
import hictkpy
from diskcache import Cache

from locus.requests.util import as_key, sanitize_keys

def hictk_request(request: dict) -> Generator[str, None, None]:
    try:
        file_req = request.get("file")
        cache_req = request.get("cache")
        regions_req = request.get("regions")
        assert file_req, f"Must specify 'file' key."
        assert cache_req, f"Must specify 'cache' key."
        assert regions_req, f"Must specify 'regions' key."
        path = file_req.get("path")
        assert path, f"Must specify file['path'] key."
        path = Path(path)
        assert path.exists(), f"File not found at {path}"

        # There is a bug either in hictkpy or nanobind
        # where the keys in dicts loaded from json are unable to bind.
        # Only hardcoded strings seem to work.
        kwargs = sanitize_keys(file_req, ["path", "resolution", "matrix_type", "matrix_unit"])
        file = hictkpy.File(**kwargs)
        cache = Cache(cache_req)
    except Exception as e:
        error = request.copy()
        error.update({"error": str(e)})
        yield error
        return
    for region_req in regions_req:
        try:
            cache_directive = region_req.get("_cache_", {})
            reuse = cache_directive.get("reuse", True)
            region = {k: v for k, v in region_req.items() if k != "_cache_"}
            key = cache_directive.get("key", as_key({**file_req, **region}))
            if not reuse or key not in cache:
                result = file.fetch(**region)
                cache[key] = result
            yield key
        except Exception as e:
            error = region_req.copy()
            error.update({"error": tr(e)})
            yield error
