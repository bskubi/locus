import json

import click

from locus.cli.request import request

@click.group
def locus():
    pass

locus.add_command(request)

if __name__ == "__main__":
    locus()
