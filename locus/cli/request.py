import json

import click
from diskcache import Cache

from locus.requests.hictk_request import hictk_request

@click.command
@click.argument("requests_jsons", nargs=-1)
def request(requests_jsons):
    for request_json in requests_jsons:
        try:
            request = json.loads(request_json)
            assert request.get("type"), "Must specify 'type' key from options: [hictk]" 
        except Exception as e:
            print(request_json)
            raise e
        if request.get("type") == "hictk":
            keys = hictk_request(request)
            for key in keys:
                print(key)