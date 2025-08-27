from typing import *
from pathlib import Path
import json

import click
import hictkpy
from diskcache import Cache

from locus.requests.util import as_key, sanitize_keys

# def HictkRequest:
#     def __init__(self, file_req: dict, region_req: dict):
#         self.file_req = file_req
#         self.cache_directive = region_req.get("_cache_", {})
#         self.overwrite = self.cache_directive.get("overwrite", False)
#         self.region = {k: v for k, v in region_req.items() if k != "_cache_"}


#     @property
#     def key(self) -> str:
#         return self.cache_directive.get("key", as_key({**self.file_req, **self.region}))

# def HictkBatchRequest:
#     def __init__(self, request: dict):
#         try:
#             file_req = request.get("file")
#             cache_req = request.get("cache")
#             regions_reqs = request.get("regions")
#             assert file_req, f"Must specify 'file' key."
#             assert cache_req, f"Must specify 'cache' key."
#             assert regions_req, f"Must specify 'regions' key."
#             path = file_req.get("path")
#             assert path, f"Must specify file['path'] key."
#             path = Path(path)
#             assert path.exists(), f"File not found at {path}"

#             # There is a bug either in hictkpy or nanobind
#             # where the keys in dicts loaded from json are unable to bind.
#             # Only hardcoded strings seem to work.
#             kwargs = sanitize_keys(file_req, ["path", "resolution", "matrix_type", "matrix_unit"])
#             self.file = hictkpy.File(**kwargs)
#             self.cache = Cache(cache_req)
#         except Exception as e:
#             error = request.copy()
#             error.update({"error": str(e)})
#             yield error
#             return
#         self.requests = []
#         for region_req in region_reqs:
#             try:
#                 self.requests.append(HictkRequest(file_req, region_req))
#             except:
#                 error = region_req.copy()
#                 error.update({"error": str(e)})
#                 yield error

#     @property
#     def keys(self) -> Generator[str, None, None]:
#         for request in self.requests:
#             yield request.key


def hictk_request_handler(request: dict) -> Generator[str, None, None]:
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
                result = file.fetch(**region).to_numpy()
                cache[key] = result
            yield key
        except Exception as e:
            error = region_req.copy()
            error.update({"error": str(e)})
            yield error
