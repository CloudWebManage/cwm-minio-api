import sys
import logging
import importlib

import asyncclick as click

from . import config, access_keys


logging.basicConfig(
    level=getattr(logging, config.CWM_LOG_LEVEL),
    handlers=[logging.StreamHandler(sys.stderr)]
)


@click.group()
async def main():
    pass


for submodule in [
    'buckets',
    'instances',
]:
    main.add_command(getattr(importlib.import_module(f'.{submodule}.router', __package__), 'main'), name=submodule.replace('_', '-'))


@main.command()
async def get_access_key():
    click.echo(await access_keys.get_access_key())


if __name__ == '__main__':
    main()
